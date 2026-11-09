-- Edit-level fact table for analysis (one row per edit).
select
    event_id,
    wiki,
    page_id,
    revision_id,
    event_date,
    event_hour,
    ts,
    change_kind,
    user_type,
    is_minor,
    is_revert,
    revert_kind,
    bytes_delta,
    abs(bytes_delta) as abs_bytes_delta,
    comment_length,
    comment_length = 0 as has_empty_comment,
    account_age_days,
    user_edit_count,
    ingest_lag_seconds
from {{ ref('stg_edits') }}
