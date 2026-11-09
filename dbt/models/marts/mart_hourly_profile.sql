-- Average edits by day of week and hour of day (UTC): the "normal" rhythm of a wiki.
with hourly as (
    select
        wiki,
        hour_start,
        isodow(hour_start) as day_of_week,
        hour(hour_start) as hour_of_day,
        edits,
        reverts
    from {{ ref('stg_wiki_hourly') }}
)

select
    wiki,
    day_of_week,
    hour_of_day,
    count(*) as hours_observed,
    round(avg(edits), 2) as avg_edits,
    round(avg(reverts), 2) as avg_reverts,
    max(edits) as max_edits
from hourly
group by 1, 2, 3
