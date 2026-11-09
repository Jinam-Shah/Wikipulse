"""Collect inputs from Postgres and the Iceberg lake, run the checks, store the results.

    python -m wikipulse.quality                       # the last finished hour
    python -m wikipulse.quality --hour 2026-11-09T05:00Z

Exit code 1 if any check fails (Airflow then marks the task failed); warnings exit 0.
Every result is stored in the `data_quality_checks` table.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import psycopg

from wikipulse.quality import checks
from wikipulse.quality.checks import CheckResult

log = logging.getLogger(__name__)
HOUR = timedelta(hours=1)


def postgres_inputs(conn: psycopg.Connection, hour: datetime) -> dict:
    latest = conn.execute("SELECT max(processed_at) FROM edits").fetchone()[0]
    per_wiki = conn.execute(
        """
        SELECT wiki, count(*), count(*) FILTER (WHERE revert_kind IS NOT NULL)
        FROM edits WHERE ts >= %s AND ts < %s GROUP BY wiki
        """,
        (hour, hour + HOUR),
    ).fetchall()
    # Previous 24 hours that actually have data (hours when the pipeline was off are
    # absent rather than zero, so they don't drag the baseline down).
    history = conn.execute(
        """
        SELECT wiki, date_trunc('hour', window_start) AS h, sum(edits)
        FROM wiki_metrics_1min
        WHERE window_start >= %s AND window_start < %s
        GROUP BY 1, 2 HAVING sum(edits) > 0
        """,
        (hour - 24 * HOUR, hour),
    ).fetchall()
    hist: dict[str, list[int]] = {}
    for wiki, _h, n in history:
        hist.setdefault(wiki, []).append(int(n))
    return {
        "latest_processed_at": latest,
        "edits": {w: int(n) for w, n, _ in per_wiki},
        "reverts": {w: int(r) for w, _, r in per_wiki},
        "history": hist,
    }


def lake_inputs(lake_dir, hour: datetime) -> dict[str, int]:
    """Edits per wiki for the hour from gold.wiki_hourly ({} if the lake has no such table)."""
    from pyiceberg.exceptions import NoSuchTableError

    from wikipulse.lake.iceberg import hour_filter
    from wikipulse.lake.paths import LakePaths, load_catalog

    try:
        table = load_catalog(LakePaths.at(lake_dir)).load_table("gold.wiki_hourly")
    except NoSuchTableError:
        return {}
    rows = table.scan(row_filter=hour_filter("hour_start", hour)).to_arrow().to_pylist()
    return {r["wiki"]: int(r["edits"]) for r in rows}


def evaluate(
    hour: datetime, now: datetime, pg: dict, lake: dict[str, int], wikis: Iterable[str]
) -> list[CheckResult]:
    results = [checks.freshness(pg["latest_processed_at"], now)]
    for wiki in sorted({*wikis, *pg["edits"], *lake}):
        edits = pg["edits"].get(wiki, 0)
        results.append(checks.volume(wiki, edits, pg["history"].get(wiki, [])))
        if edits or wiki in lake:
            results.append(checks.reconciliation(wiki, edits, lake.get(wiki)))
            results.append(checks.revert_rate(wiki, edits, pg["reverts"].get(wiki, 0)))
    return results


def store(conn: psycopg.Connection, hour: datetime, results: list[CheckResult]) -> None:
    with conn.transaction():
        for r in results:
            conn.execute(
                """
                INSERT INTO data_quality_checks
                    (target_hour, check_name, wiki, status, observed, threshold, message, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    hour,
                    r.name,
                    r.wiki,
                    r.status,
                    r.observed,
                    r.threshold,
                    r.message,
                    json.dumps(r.details, default=str),
                ),
            )


def run(database_url: str, lake_dir, hour: datetime, wikis: Iterable[str], now: datetime):
    with psycopg.connect(database_url, autocommit=True) as conn:
        pg = postgres_inputs(conn, hour)
        results = evaluate(hour, now, pg, lake_inputs(lake_dir, hour), wikis)
        store(conn, hour, results)
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m wikipulse.quality")
    p.add_argument("--hour", help="hour to check (ISO 8601, UTC); default: last finished hour")
    args = p.parse_args(argv)
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("pyiceberg").setLevel("WARNING")

    from wikipulse.config import load_settings
    from wikipulse.db.__main__ import upgrade
    from wikipulse.lake.export import floor_hour

    settings = load_settings()
    upgrade(settings.database_url)
    now = datetime.now(UTC)
    if args.hour:
        hour = floor_hour(datetime.fromisoformat(args.hour.replace("Z", "+00:00")))
    else:
        hour = floor_hour(now) - HOUR

    results = run(settings.database_url, settings.lake_dir, hour, settings.wikis, now)
    for r in results:
        print(f"{r.status.upper():4}  {r.name:20} {r.wiki or '-':12} {r.message}")
    failed = [r for r in results if r.status == "fail"]
    if failed:
        print(f"{len(failed)} check(s) failed for {hour:%Y-%m-%d %H}:00", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
