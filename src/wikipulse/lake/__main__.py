"""Lakehouse command line.

python -m wikipulse.lake run                 # last 3 finished hours: export + bronze/silver/gold
python -m wikipulse.lake run --hours 24
python -m wikipulse.lake run --start 2026-11-09T00:00Z --end 2026-11-09T06:00Z
python -m wikipulse.lake export --hours 3    # Parquet only
python -m wikipulse.lake dbt build           # dbt marts + tests (any dbt command works)
python -m wikipulse.lake dbt docs generate
python -m wikipulse.lake status              # Iceberg snapshots and row counts
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from wikipulse.config import load_settings

DBT_DIR = Path(os.environ.get("WIKIPULSE_DBT_DIR") or Path(__file__).resolve().parents[3] / "dbt")


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _hours(args) -> list[datetime]:
    from wikipulse.lake.export import complete_hours, hours_between

    if args.start:
        end = _parse_dt(args.end) if args.end else datetime.now(UTC)
        return hours_between(_parse_dt(args.start), end)
    return complete_hours(datetime.now(UTC), args.hours)


def dbt_env(paths) -> dict[str, str]:
    return {
        "WIKIPULSE_DUCKDB_PATH": str(paths.duckdb),
        "WIKIPULSE_ICEBERG_CATALOG_URI": paths.catalog_uri,
        "WIKIPULSE_ICEBERG_WAREHOUSE": str(paths.warehouse),
        "DBT_PROFILES_DIR": str(DBT_DIR),
    }


def run_dbt(paths, dbt_args: list[str]) -> int:
    from dbt.cli.main import dbtRunner

    os.environ.update(dbt_env(paths))
    if not any(a == "--project-dir" for a in dbt_args):
        dbt_args = [*dbt_args, "--project-dir", str(DBT_DIR)]
    result = dbtRunner().invoke(dbt_args)
    return 0 if result.success else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m wikipulse.lake")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("run", "export"):
        sp = sub.add_parser(name)
        sp.add_argument("--hours", type=int, default=3, help="last N finished hours")
        sp.add_argument("--start", help="first hour (ISO 8601, UTC)")
        sp.add_argument("--end", help="end hour, exclusive (default: now)")
    sub.add_parser("status")
    dbt = sub.add_parser("dbt", help="run a dbt command against the lake")
    dbt.add_argument("dbt_args", nargs=argparse.REMAINDER)
    args = p.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("pyiceberg").setLevel("WARNING")
    settings = load_settings()

    from wikipulse.lake.paths import LakePaths, load_catalog

    paths = LakePaths.at(settings.lake_dir)

    if args.cmd == "dbt":
        return run_dbt(paths, args.dbt_args)

    if args.cmd == "status":
        from wikipulse.lake.iceberg import ensure_tables

        for ident, table in ensure_tables(load_catalog(paths)).items():
            snap = table.current_snapshot()
            rows = table.scan().to_arrow().num_rows if snap else 0
            n = len(table.metadata.snapshots)
            print(f"{ident:18} rows={rows:<9} snapshots={n:<5} location={table.location()}")
        return 0

    from wikipulse.db import connect
    from wikipulse.lake.export import export_hours

    hours = _hours(args)
    now = datetime.now(UTC)
    with connect(settings.database_url) as conn:
        exports = export_hours(conn, paths, hours, now=now, retention_days=settings.retention_days)
    for e in exports:
        print(f"exported {e.hour:%Y-%m-%d %H}:00  rows={e.rows:<6} -> {e.path}")
    if args.cmd == "export":
        return 0

    from wikipulse.lake.iceberg import build_hour, ensure_tables, expire_old_snapshots

    tables = ensure_tables(load_catalog(paths))
    for hour in hours:
        b = build_hour(tables, paths, hour)
        print(
            f"built    {hour:%Y-%m-%d %H}:00  bronze={b.bronze} silver={b.silver} "
            f"page_hourly={b.page_hourly} wiki_hourly={b.wiki_hourly}"
        )
    expire_old_snapshots(tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())
