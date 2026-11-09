"""Breaking News Radar service.

python -m wikipulse.radar                    # run forever (every 30 s: new windows, confirmations)
python -m wikipulse.radar --once             # process what's available, then exit (hosted mode)
python -m wikipulse.radar --rebuild --since 2026-11-08T00:00Z --once
                                             # forget all trends and replay history
python -m wikipulse.radar --no-pageviews     # skip page-view confirmation
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from prometheus_client import Counter, Gauge

from wikipulse.radar import store
from wikipulse.radar.detector import Params
from wikipulse.radar.pageviews import (
    DEFAULT_RULE,
    HOUR,
    PageviewsClient,
    Rule,
    floor_hour,
    judge,
    project_for_wiki,
)

log = logging.getLogger("wikipulse.radar")

WINDOWS = Counter("wikipulse_radar_windows_total", "5-minute windows evaluated")
TRENDS = Counter("wikipulse_radar_trends_total", "Trend lifecycle events", ["event"])
OPEN = Gauge("wikipulse_radar_open_trends", "Trends currently open")
WATERMARK = Gauge("wikipulse_radar_watermark_timestamp_seconds", "Last window evaluated")
VIEWS = Counter("wikipulse_radar_view_checks_total", "Page-view checks by result", ["status"])


def process_available(conn, p: Params, grace: timedelta, since: datetime | None, limit=288):
    results = []
    for t in store.next_windows(conn, grace, limit, since):
        r = store.process_window(conn, t, p)
        results.append(r)
        WINDOWS.inc()
        TRENDS.labels("opened").inc(r.opened)
        TRENDS.labels("closed").inc(r.closed)
        WATERMARK.set(t.timestamp())
        if r.opened or r.closed:
            log.info("window %s: pages=%d opened=%d closed=%d", t, r.pages, r.opened, r.closed)
    OPEN.set(store.open_trend_count(conn))
    return results


PENDING = """
SELECT trend_id, wiki, page_title, started_at FROM trends
WHERE views_status = 'pending' AND started_at >= %(oldest)s
  AND (views_checked_at IS NULL OR views_checked_at < %(recheck)s)
ORDER BY started_at LIMIT %(limit)s
"""


def confirm_pending(
    conn, client: PageviewsClient, now: datetime, rule: Rule = DEFAULT_RULE, limit: int = 20
) -> int:
    rows = conn.execute(
        PENDING,
        {"oldest": now - timedelta(days=3), "recheck": now - timedelta(minutes=30), "limit": limit},
    ).fetchall()
    done = 0
    for trend_id, wiki, title, started_at in rows:
        project = project_for_wiki(wiki)
        start_hour = floor_hour(started_at)
        first = start_hour - HOUR * rule.baseline_hours
        last = start_hour + HOUR * (rule.window_hours - 1)
        views = client.hourly(project, title, first, last)
        verdict = judge(views, started_at, now, rule)
        with conn.transaction():
            if views:
                conn.cursor().executemany(
                    """
                    INSERT INTO page_views_hourly (project, page_title, hour, views)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (project, page_title, hour)
                    DO UPDATE SET views = EXCLUDED.views, fetched_at = now()
                    """,
                    [(project, title, h, v) for h, v in views.items()],
                )
            conn.execute(
                """
                UPDATE trends SET views_status = %s, views_peak = %s, views_baseline = %s,
                    views_ratio = %s, views_checked_at = now(),
                    confirmed_by_views = CASE %s WHEN 'confirmed' THEN true
                                                 WHEN 'not_confirmed' THEN false END
                WHERE trend_id = %s
                """,
                (
                    verdict.status,
                    verdict.peak,
                    verdict.baseline,
                    verdict.ratio,
                    verdict.status,
                    trend_id,
                ),
            )
        VIEWS.labels(verdict.status).inc()
        done += 1
    return done


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wikipulse-radar", description=__doc__.splitlines()[0])
    p.add_argument("--once", action="store_true", help="process what's available, then exit")
    p.add_argument("--rebuild", action="store_true", help="delete trends and baselines first")
    p.add_argument("--since", help="first window when there is no watermark (ISO 8601, UTC)")
    p.add_argument("--grace-seconds", type=float, default=120)
    p.add_argument("--no-pageviews", action="store_true")
    p.add_argument("--no-metrics-server", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel("WARNING")

    from wikipulse.config import load_settings
    from wikipulse.db.__main__ import upgrade

    settings = load_settings()
    upgrade(settings.database_url)
    if not args.no_metrics_server:
        from prometheus_client import start_http_server

        start_http_server(settings.radar_metrics_port)
    since = datetime.fromisoformat(args.since.replace("Z", "+00:00")) if args.since else None
    params = Params()
    grace = timedelta(seconds=args.grace_seconds)
    client = None if args.no_pageviews else PageviewsClient(settings.user_agent)

    stopping = {"flag": False}

    def _stop(signum: int, _frame: Any) -> None:
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)

    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        if args.rebuild:
            store.reset(conn)
            log.info("radar state reset")
        last_confirm = last_prune = float("-inf")
        while not stopping["flag"]:
            processed = process_available(conn, params, grace, since)
            now = datetime.now(UTC)
            if client and time.monotonic() - last_confirm >= 300:
                last_confirm = time.monotonic()
                try:
                    confirm_pending(conn, client, now)
                except Exception as exc:  # confirmation is best-effort; detection must go on
                    log.warning("page-view confirmation failed: %s", exc)
            if time.monotonic() - last_prune >= 3600:
                last_prune = time.monotonic()
                store.prune_baselines(conn, now - timedelta(days=3))
            if args.once and not processed:
                break
            if not processed:
                time.sleep(30)
        log.info("radar stopped; watermark=%s", store.watermark(conn))
    return 0


def cli() -> int:
    return main()
