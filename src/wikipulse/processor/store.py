"""Postgres writes for the stream processor.

Every batch is one transaction:
1. insert new edits (ON CONFLICT (event_id) DO NOTHING -> replays are no-ops),
2. flag reverts whose content hash matches an earlier revision of the same page,
3. recompute the 5-minute page windows and 1-minute wiki windows the batch touched,
   from the `edits` table, and upsert them.

Because step 3 recomputes from stored rows instead of adding to counters, the
metrics are a pure function of `edits`: replaying any amount of the topic, in any
order, gives the same numbers (docs/DECISIONS.md D-013).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import psycopg

from wikipulse.processor.revert import comment_looks_like_revert
from wikipulse.producer.schemas import EditEvent

EDIT_COLUMNS = (
    "event_id, wiki, change_kind, page_id, page_title, namespace, revision_id, "
    "parent_revision_id, rev_sha1, user_name, user_id, is_bot, is_anon, user_edit_count, "
    "user_registered_at, comment, is_minor, bytes_old, bytes_new, ts, event_dt, ingested_at, "
    "revert_kind"
)

INSERT_EDITS = f"""
INSERT INTO edits ({EDIT_COLUMNS})
SELECT {EDIT_COLUMNS} FROM jsonb_populate_recordset(NULL::edits, %(rows)s::jsonb)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""

# Exact revert: content hash equals an older revision's hash on the same page.
# Checks every edit on the pages this batch touched (not only the new rows), so the
# result is the same whatever order events arrive in. Returns the rows it flagged so
# their windows can be recomputed too.
MARK_SHA1_REVERTS = """
WITH pages AS (
    SELECT DISTINCT wiki, page_id FROM edits WHERE event_id = ANY(%(ids)s)
)
UPDATE edits e SET revert_kind = 'sha1'
FROM pages
WHERE e.wiki = pages.wiki AND e.page_id = pages.page_id
  AND e.rev_sha1 IS NOT NULL
  AND e.revert_kind IS DISTINCT FROM 'sha1'
  AND EXISTS (
      SELECT 1 FROM edits p
      WHERE p.wiki = e.wiki AND p.page_id = e.page_id
        AND p.rev_sha1 = e.rev_sha1 AND p.revision_id < e.revision_id
  )
RETURNING e.event_id
"""

_BIN_ORIGIN = "TIMESTAMPTZ '2000-01-01 00:00:00+00'"

RECOMPUTE_PAGE_WINDOWS = f"""
WITH affected AS (
    SELECT DISTINCT wiki, page_id, date_bin('5 minutes', ts, {_BIN_ORIGIN}) AS window_start
    FROM edits WHERE event_id = ANY(%(ids)s)
)
INSERT INTO page_metrics_5min AS m (
    wiki, page_id, window_start, page_title, edits, distinct_editors,
    bot_edits, anon_edits, reverts, bytes_delta, updated_at
)
SELECT
    a.wiki, a.page_id, a.window_start,
    (array_agg(e.page_title ORDER BY e.ts DESC))[1],
    count(*),
    count(DISTINCT coalesce(e.user_name, 'hidden:' || e.event_id)),
    count(*) FILTER (WHERE e.is_bot),
    count(*) FILTER (WHERE e.is_anon),
    count(*) FILTER (WHERE e.revert_kind IS NOT NULL),
    coalesce(sum(coalesce(e.bytes_new, 0) - coalesce(e.bytes_old, 0)), 0),
    now()
FROM affected a
JOIN edits e
  ON e.wiki = a.wiki AND e.page_id = a.page_id
 AND e.ts >= a.window_start AND e.ts < a.window_start + interval '5 minutes'
GROUP BY a.wiki, a.page_id, a.window_start
ON CONFLICT (wiki, page_id, window_start) DO UPDATE SET
    page_title = EXCLUDED.page_title,
    edits = EXCLUDED.edits,
    distinct_editors = EXCLUDED.distinct_editors,
    bot_edits = EXCLUDED.bot_edits,
    anon_edits = EXCLUDED.anon_edits,
    reverts = EXCLUDED.reverts,
    bytes_delta = EXCLUDED.bytes_delta,
    updated_at = EXCLUDED.updated_at
"""

RECOMPUTE_WIKI_WINDOWS = f"""
WITH affected AS (
    SELECT DISTINCT wiki, date_bin('1 minute', ts, {_BIN_ORIGIN}) AS window_start
    FROM edits WHERE event_id = ANY(%(ids)s)
)
INSERT INTO wiki_metrics_1min AS m (
    wiki, window_start, edits, new_pages, bot_edits, anon_edits, reverts,
    distinct_pages, distinct_editors, bytes_delta, updated_at
)
SELECT
    a.wiki, a.window_start,
    count(*),
    count(*) FILTER (WHERE e.change_kind = 'create'),
    count(*) FILTER (WHERE e.is_bot),
    count(*) FILTER (WHERE e.is_anon),
    count(*) FILTER (WHERE e.revert_kind IS NOT NULL),
    count(DISTINCT e.page_id),
    count(DISTINCT coalesce(e.user_name, 'hidden:' || e.event_id)),
    coalesce(sum(coalesce(e.bytes_new, 0) - coalesce(e.bytes_old, 0)), 0),
    now()
FROM affected a
JOIN edits e
  ON e.wiki = a.wiki
 AND e.ts >= a.window_start AND e.ts < a.window_start + interval '1 minute'
GROUP BY a.wiki, a.window_start
ON CONFLICT (wiki, window_start) DO UPDATE SET
    edits = EXCLUDED.edits,
    new_pages = EXCLUDED.new_pages,
    bot_edits = EXCLUDED.bot_edits,
    anon_edits = EXCLUDED.anon_edits,
    reverts = EXCLUDED.reverts,
    distinct_pages = EXCLUDED.distinct_pages,
    distinct_editors = EXCLUDED.distinct_editors,
    bytes_delta = EXCLUDED.bytes_delta,
    updated_at = EXCLUDED.updated_at
"""

PRUNE_EDITS = "DELETE FROM edits WHERE ts < %(cutoff)s"


@dataclass(frozen=True)
class BatchResult:
    inserted: int
    duplicates: int
    too_old: int
    inserted_events: tuple[EditEvent, ...] = ()


def edit_row(ev: EditEvent) -> dict:
    row = ev.model_dump(mode="json")
    row["revert_kind"] = "comment" if comment_looks_like_revert(ev.comment) else None
    return row


class PostgresStore:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def write_batch(self, events: Sequence[EditEvent], cutoff: datetime) -> BatchResult:
        """Write a batch atomically. Events older than `cutoff` are dropped: their windows
        may already be pruned, and recomputing them would undercount."""
        fresh = [ev for ev in events if ev.ts >= cutoff]
        too_old = len(events) - len(fresh)
        if not fresh:
            return BatchResult(0, 0, too_old)

        # Dedupe inside the batch too (a replay can put the same event twice in one batch).
        unique = list({ev.event_id: ev for ev in fresh}.values())
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(INSERT_EDITS, {"rows": json.dumps([edit_row(ev) for ev in unique])})
            new_ids = [r[0] for r in cur.fetchall()]
            if new_ids:
                cur.execute(MARK_SHA1_REVERTS, {"ids": new_ids})
                flagged = [r[0] for r in cur.fetchall()]
                params = {"ids": list({*new_ids, *flagged})}
                cur.execute(RECOMPUTE_PAGE_WINDOWS, params)
                cur.execute(RECOMPUTE_WIKI_WINDOWS, params)
        new = set(new_ids)
        return BatchResult(
            inserted=len(new_ids),
            duplicates=len(fresh) - len(new_ids),
            too_old=too_old,
            inserted_events=tuple(ev for ev in unique if ev.event_id in new),
        )

    def prune(self, cutoff: datetime) -> int:
        """Delete raw edits older than the cutoff. Window metrics are kept."""
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(PRUNE_EDITS, {"cutoff": cutoff})
            return cur.rowcount
