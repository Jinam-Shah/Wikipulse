-- Reconciliation: gold hourly aggregates (summed per day) must equal a direct count of
-- silver edits. Any row returned is a mismatch.
with from_gold as (
    select wiki, activity_date, edits, reverts from {{ ref('mart_wiki_daily') }}
),

from_silver as (
    select
        wiki,
        event_date as activity_date,
        count(*) as edits,
        count(*) filter (where is_revert) as reverts
    from {{ ref('stg_edits') }}
    group by 1, 2
)

select *
from from_gold g
full outer join from_silver s using (wiki, activity_date)
where g.edits is distinct from s.edits or g.reverts is distinct from s.reverts
