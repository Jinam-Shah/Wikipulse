select
    wiki, hour_start,
    edits, new_pages, bot_edits, anon_edits, reverts,
    distinct_pages, distinct_editors, bytes_delta
from {{ source('gold', 'wiki_hourly') }}
