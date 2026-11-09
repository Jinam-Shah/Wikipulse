select
    wiki, page_id, page_title, hour_start,
    edits, distinct_editors, bot_edits, anon_edits, reverts, bytes_delta
from {{ source('gold', 'page_hourly') }}
