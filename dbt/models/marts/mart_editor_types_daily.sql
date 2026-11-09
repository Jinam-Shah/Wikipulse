-- How bots, anonymous and registered editors differ, per day.
select
    wiki,
    event_date as activity_date,
    user_type,
    count(*) as edits,
    count(*) filter (where is_revert) as reverts,
    round(count(*) filter (where is_revert) / count(*), 4) as revert_rate,
    round(avg(abs(bytes_delta)), 1) as avg_abs_bytes_delta,
    round(avg(case when comment_length = 0 then 1 else 0 end), 4) as empty_comment_share
from {{ ref('stg_edits') }}
group by 1, 2, 3
