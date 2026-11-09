"""Radar end to end on Postgres: edits -> 5-minute windows -> trends -> page-view confirmation."""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest

from scenarios import LONG_AGO, T0, edit

pytestmark = pytest.mark.integration
psycopg = pytest.importorskip("psycopg")

SPIKE_AT = 5 * 3600  # seconds after T0 (12:00) -> 17:00


def scenario():
    events, i = [], 0

    def add(**kw):
        nonlocal i
        i += 1
        events.append(edit(i, **kw))

    for k in range(12):  # page 1: steady background, 2 edits/hour for 6 hours
        add(page=1, secs=k * 1800, user="Alice")
    for k in range(12):  # page 2: breaking news - 12 edits by 6 editors within 5 minutes
        add(page=2, secs=SPIKE_AT + 10 + k * 20, user=f"Editor{k % 6}")
    for k in range(20):  # page 3: one person making 20 quick edits - NOT news
        add(page=3, secs=SPIKE_AT + 15 + k * 10, user="Solo")
    add(page=1, secs=7 * 3600, user="Alice")  # stream moves on, so the windows close
    return events


@pytest.fixture
def db(fresh_db_url):
    from wikipulse.processor.store import PostgresStore

    store = PostgresStore(psycopg.connect(fresh_db_url, autocommit=True))
    store.write_batch(scenario(), cutoff=LONG_AGO)
    conn = psycopg.connect(fresh_db_url, autocommit=True)
    yield conn
    conn.close()


def run_radar(conn):
    from wikipulse.radar.detector import Params
    from wikipulse.radar.main import process_available

    return process_available(conn, Params(), timedelta(minutes=2), since=None, limit=10_000)


def trends(conn):
    return conn.execute(
        "SELECT page_id, status, started_at, peak_edits, distinct_editors, total_edits, ended_at "
        "FROM trends ORDER BY trend_id"
    ).fetchall()


def test_only_the_multi_editor_spike_becomes_a_trend(db):
    results = run_radar(db)
    assert len(results) > 70  # every 5-minute window from 12:00 to ~18:55
    found = trends(db)
    assert len(found) == 1
    page, status, started, peak_edits, editors, total, ended = found[0]
    assert page == 2 and status == "ended"
    assert started == T0 + timedelta(seconds=SPIKE_AT)
    assert (peak_edits, editors, total) == (12, 6, 12)
    assert ended == started + timedelta(minutes=5)

    timeline = db.execute(
        "SELECT window_start, edits FROM trend_windows ORDER BY window_start"
    ).fetchall()
    assert timeline[0] == (started, 12) and len(timeline) == 4  # spike + 3 quiet windows


def test_rerun_is_a_no_op_and_rebuild_reproduces_the_same_trends(db):
    from wikipulse.radar import store

    run_radar(db)
    first = trends(db)
    assert run_radar(db) == []  # watermark: nothing left to do
    assert trends(db) == first

    store.reset(db)
    run_radar(db)
    assert [t[:6] for t in trends(db)] == [t[:6] for t in first]


def test_pageview_confirmation_is_stored(db):
    from wikipulse.radar.main import confirm_pending
    from wikipulse.radar.pageviews import PageviewsClient

    run_radar(db)
    start_hour = T0 + timedelta(seconds=SPIKE_AT)

    def handler(req):
        items = [
            {"timestamp": (start_hour - timedelta(hours=h)).strftime("%Y%m%d%H"), "views": 80}
            for h in range(1, 72)
        ]
        items.append({"timestamp": start_hour.strftime("%Y%m%d%H"), "views": 2400})
        return httpx.Response(200, json={"items": items})

    client = PageviewsClient(
        "WikiPulse-test/0",
        httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=lambda s: None,
    )
    now = start_hour + timedelta(hours=3)
    assert confirm_pending(db, client, now) == 1
    status, confirmed, peak, ratio = db.execute(
        "SELECT views_status, confirmed_by_views, views_peak, views_ratio FROM trends"
    ).fetchone()
    assert (status, confirmed, peak, ratio) == ("confirmed", True, 2400, 30.0)
    assert db.execute("SELECT count(*) FROM page_views_hourly").fetchone()[0] == 72
    assert confirm_pending(db, client, now) == 0  # nothing pending any more
