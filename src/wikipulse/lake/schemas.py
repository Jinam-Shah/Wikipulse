"""Arrow schemas for the raw export and the Iceberg tables.

Timestamps are microseconds in UTC everywhere (Iceberg `timestamptz`).
"""

from __future__ import annotations

import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")

# Mirrors the Postgres `edits` table (Phase 2), in a stable column order.
RAW_EDITS = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("wiki", pa.string(), nullable=False),
        pa.field("change_kind", pa.string(), nullable=False),
        pa.field("page_id", pa.int64(), nullable=False),
        pa.field("page_title", pa.string(), nullable=False),
        pa.field("namespace", pa.int32(), nullable=False),
        pa.field("revision_id", pa.int64(), nullable=False),
        pa.field("parent_revision_id", pa.int64()),
        pa.field("rev_sha1", pa.string()),
        pa.field("user_name", pa.string()),
        pa.field("user_id", pa.int64()),
        pa.field("is_bot", pa.bool_(), nullable=False),
        pa.field("is_anon", pa.bool_(), nullable=False),
        pa.field("user_edit_count", pa.int32()),
        pa.field("user_registered_at", TS),
        pa.field("comment", pa.string()),
        pa.field("is_minor", pa.bool_(), nullable=False),
        pa.field("bytes_old", pa.int32()),
        pa.field("bytes_new", pa.int32()),
        pa.field("ts", TS, nullable=False),
        pa.field("event_dt", TS, nullable=False),
        pa.field("ingested_at", TS, nullable=False),
        pa.field("revert_kind", pa.string()),
        pa.field("risk_score", pa.float32()),
        pa.field("model_version", pa.string()),
        pa.field("processed_at", TS, nullable=False),
    ]
)
RAW_COLUMNS = [f.name for f in RAW_EDITS]

# Bronze = raw + load bookkeeping.
BRONZE_EDITS = RAW_EDITS.append(pa.field("_export_hour", TS, nullable=False)).append(
    pa.field("_loaded_at", TS, nullable=False)
)

SILVER_EDITS = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("wiki", pa.string(), nullable=False),
        pa.field("page_id", pa.int64(), nullable=False),
        pa.field("page_title", pa.string(), nullable=False),
        pa.field("namespace", pa.int32(), nullable=False),
        pa.field("change_kind", pa.string(), nullable=False),
        pa.field("revision_id", pa.int64(), nullable=False),
        pa.field("parent_revision_id", pa.int64()),
        pa.field("user_name", pa.string()),
        pa.field("user_type", pa.string(), nullable=False),  # bot | anon | registered
        pa.field("is_bot", pa.bool_(), nullable=False),
        pa.field("is_anon", pa.bool_(), nullable=False),
        pa.field("user_edit_count", pa.int32()),
        pa.field("account_age_days", pa.int32()),
        pa.field("comment", pa.string()),
        pa.field("comment_length", pa.int32(), nullable=False),
        pa.field("is_minor", pa.bool_(), nullable=False),
        pa.field("bytes_old", pa.int32()),
        pa.field("bytes_new", pa.int32()),
        pa.field("bytes_delta", pa.int64(), nullable=False),
        pa.field("is_revert", pa.bool_(), nullable=False),
        pa.field("revert_kind", pa.string()),
        pa.field("ts", TS, nullable=False),
        pa.field("event_hour", TS, nullable=False),
        pa.field("ingest_lag_seconds", pa.float64(), nullable=False),
        pa.field("_loaded_at", TS, nullable=False),
    ]
)

GOLD_PAGE_HOURLY = pa.schema(
    [
        pa.field("wiki", pa.string(), nullable=False),
        pa.field("page_id", pa.int64(), nullable=False),
        pa.field("page_title", pa.string(), nullable=False),
        pa.field("hour_start", TS, nullable=False),
        pa.field("edits", pa.int64(), nullable=False),
        pa.field("distinct_editors", pa.int64(), nullable=False),
        pa.field("bot_edits", pa.int64(), nullable=False),
        pa.field("anon_edits", pa.int64(), nullable=False),
        pa.field("reverts", pa.int64(), nullable=False),
        pa.field("bytes_delta", pa.int64(), nullable=False),
        pa.field("_loaded_at", TS, nullable=False),
    ]
)

GOLD_WIKI_HOURLY = pa.schema(
    [
        pa.field("wiki", pa.string(), nullable=False),
        pa.field("hour_start", TS, nullable=False),
        pa.field("edits", pa.int64(), nullable=False),
        pa.field("new_pages", pa.int64(), nullable=False),
        pa.field("bot_edits", pa.int64(), nullable=False),
        pa.field("anon_edits", pa.int64(), nullable=False),
        pa.field("reverts", pa.int64(), nullable=False),
        pa.field("distinct_pages", pa.int64(), nullable=False),
        pa.field("distinct_editors", pa.int64(), nullable=False),
        pa.field("bytes_delta", pa.int64(), nullable=False),
        pa.field("_loaded_at", TS, nullable=False),
    ]
)
