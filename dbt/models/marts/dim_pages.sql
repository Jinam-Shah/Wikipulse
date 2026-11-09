-- One row per page seen in the lake, with its latest title.
select
    wiki,
    page_id,
    arg_max(page_title, ts) as page_title,
    min(ts) as first_seen_at,
    max(ts) as last_seen_at,
    count(*) as total_edits,
    bool_or(change_kind = 'create') as created_in_lake
from {{ ref('stg_edits') }}
group by wiki, page_id
