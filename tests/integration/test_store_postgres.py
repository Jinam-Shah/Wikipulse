"""Window metrics are correct, and replays / reordering never change them."""

from __future__ import annotations

import random
from datetime import timedelta

import pytest

from scenarios import LONG_AGO, SCENARIO, T0, edits_count, page_metrics, wiki_metrics

pytestmark = pytest.mark.integration
psycopg = pytest.importorskip("psycopg")


def store_for(url):
    from wikipulse.processor.store import PostgresStore

    return PostgresStore(psycopg.connect(url, autocommit=True))


def test_window_metrics_are_correct(fresh_db_url):
    result = store_for(fresh_db_url).write_batch(SCENARIO, cutoff=LONG_AGO)
    assert (result.inserted, result.duplicates, result.too_old) == (7, 0, 0)

    w1, w2 = T0, T0 + timedelta(minutes=5)
    assert page_metrics(fresh_db_url) == [
        # page, window, title, edits, editors, bots, anon, reverts, bytes_delta
        (1, w1, "Page 1", 5, 4, 1, 1, 2, 100 - 900 + 900 + 5 + 10),
        (1, w2, "Page 1", 1, 1, 0, 0, 0, 10),
        (2, w1, "Page 2", 1, 1, 0, 0, 0, 500),
    ]

    with psycopg.connect(fresh_db_url) as conn:
        kinds = dict(conn.execute("SELECT event_id, revert_kind FROM edits").fetchall())
    assert kinds["evt-3"] == "sha1" and kinds["evt-5"] == "comment"
    assert kinds["evt-1"] is None  # the version reverted *to* is not itself a revert

    minutes = {row[0]: row[1:] for row in wiki_metrics(fresh_db_url)}
    # 12:00 minute: evt-1 (Alice, p1) and evt-7 (Dave, p2, page creation)
    assert minutes[T0] == (2, 1, 0, 0, 0, 2, 2, 100 + 500)
    # 12:01 minute: evt-2 (temp account) and evt-3 (sha1 revert)
    assert minutes[T0 + timedelta(minutes=1)] == (2, 0, 0, 1, 1, 1, 2, -900 + 900)


def test_replay_and_duplicates_change_nothing(fresh_db_url):
    store = store_for(fresh_db_url)
    store.write_batch(SCENARIO, cutoff=LONG_AGO)
    before = (page_metrics(fresh_db_url), wiki_metrics(fresh_db_url), edits_count(fresh_db_url))

    again = store.write_batch(SCENARIO, cutoff=LONG_AGO)  # full replay
    assert (again.inserted, again.duplicates) == (0, 7)
    store.write_batch(SCENARIO[:3] + SCENARIO[:3], cutoff=LONG_AGO)  # dupes inside a batch

    after = (page_metrics(fresh_db_url), wiki_metrics(fresh_db_url), edits_count(fresh_db_url))
    assert after == before


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_any_order_and_batching_gives_the_same_result(fresh_db_url, seed):
    from wikipulse.processor.store import PostgresStore

    store_for(fresh_db_url).write_batch(SCENARIO, cutoff=LONG_AGO)
    expected = (page_metrics(fresh_db_url), wiki_metrics(fresh_db_url))

    # Same events, shuffled and split into small batches, into a second database
    # (reuse the same one after truncating).
    with psycopg.connect(fresh_db_url, autocommit=True) as conn:
        conn.execute("TRUNCATE edits, page_metrics_5min, wiki_metrics_1min")
    events = SCENARIO[:]
    random.Random(seed).shuffle(events)
    store = PostgresStore(psycopg.connect(fresh_db_url, autocommit=True))
    for i in range(0, len(events), 2):
        store.write_batch(events[i : i + 2], cutoff=LONG_AGO)

    assert (page_metrics(fresh_db_url), wiki_metrics(fresh_db_url)) == expected


def test_events_older_than_retention_are_dropped(fresh_db_url):
    result = store_for(fresh_db_url).write_batch(SCENARIO, cutoff=T0 + timedelta(seconds=100))
    assert (result.inserted, result.too_old) == (3, 4)


def test_prune_removes_old_edits_but_keeps_metrics(fresh_db_url):
    store = store_for(fresh_db_url)
    store.write_batch(SCENARIO, cutoff=LONG_AGO)
    metrics_before = page_metrics(fresh_db_url)
    deleted = store.prune(T0 + timedelta(seconds=250))
    assert deleted == 6 and edits_count(fresh_db_url) == 1
    assert page_metrics(fresh_db_url) == metrics_before


def test_migrations_upgrade_is_idempotent_and_downgrade_works(fresh_db_url):
    from alembic import command

    from wikipulse.db.__main__ import alembic_config, upgrade

    upgrade(fresh_db_url)  # second run: no-op
    command.downgrade(alembic_config(fresh_db_url), "base")
    with psycopg.connect(fresh_db_url) as conn:
        assert conn.execute("SELECT to_regclass('edits')").fetchone()[0] is None
    upgrade(fresh_db_url)
