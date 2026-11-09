"""Synthetic edit scenario shared by the integration tests, plus DB read helpers."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from conftest import make_page_change
from wikipulse.producer.normalize import Filters, normalize
from wikipulse.producer.schemas import EditEvent

FILTERS = Filters(frozenset({"enwiki"}), frozenset({0}))
T0 = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)  # a 5-minute window boundary
LONG_AGO = T0 - timedelta(days=30)


def edit(
    i: int,
    *,
    page: int = 1,
    secs: float = 0,
    user: str = "Alice",
    bot: bool = False,
    temp: bool = False,
    sha1: str | None = None,
    size_old: int = 1000,
    size_new: int = 1010,
    comment: str = "copyedit",
    kind: str = "edit",
) -> EditEvent:
    ts = (T0 + timedelta(seconds=secs)).isoformat().replace("+00:00", "Z")
    doc = make_page_change(
        page_change_kind=kind,
        meta={"id": f"evt-{i}", "dt": ts},
        page={"page_id": page, "page_title": f"Page_{page}"},
        performer={
            "user_text": user,
            "user_id": 0 if temp else 100 + i,
            "is_temp": temp,
            "is_bot": bot,
            "groups": ["bot"] if bot else ["user"],
        },
        revision={
            "rev_id": 10_000 + i,
            "rev_dt": ts,
            "rev_size": size_new,
            "rev_sha1": sha1 or f"sha-{i}",
            "comment": comment,
        },
        prior_state={"revision": {"rev_size": size_old}},
    )
    ev = normalize(json.dumps(doc), FILTERS)
    assert isinstance(ev, EditEvent), ev
    return ev


SCENARIO = [
    # Page 1, window 12:00-12:05
    edit(1, page=1, secs=10, user="Alice", sha1="A", size_old=1000, size_new=1100),
    edit(2, page=1, secs=60, user="Vandal", temp=True, sha1="B", size_old=1100, size_new=200),
    edit(3, page=1, secs=90, user="Bob", sha1="A", size_old=200, size_new=1100),  # sha1 revert
    edit(4, page=1, secs=120, user="FixBot", bot=True, sha1="C", size_old=1100, size_new=1105),
    edit(5, page=1, secs=200, user="Alice", sha1="D", comment="rv vandalism"),  # comment revert
    # Page 1, next window 12:05-12:10 (300 s is exactly the boundary)
    edit(6, page=1, secs=300, user="Carol", sha1="E"),
    # Page 2, created in window 1
    edit(7, page=2, secs=30, user="Dave", sha1="F", kind="create", size_old=0, size_new=500),
]


def page_metrics(url):
    import psycopg

    with psycopg.connect(url) as conn:
        return conn.execute(
            "SELECT page_id, window_start, page_title, edits, distinct_editors, bot_edits, "
            "anon_edits, reverts, bytes_delta FROM page_metrics_5min ORDER BY 1, 2"
        ).fetchall()


def wiki_metrics(url):
    import psycopg

    with psycopg.connect(url) as conn:
        return conn.execute(
            "SELECT window_start, edits, new_pages, bot_edits, anon_edits, reverts, "
            "distinct_pages, distinct_editors, bytes_delta FROM wiki_metrics_1min ORDER BY 1"
        ).fetchall()


def edits_count(url):
    import psycopg

    with psycopg.connect(url) as conn:
        return conn.execute("SELECT count(*) FROM edits").fetchone()[0]
