-- The 25 most-edited pages per wiki per day (feeds the Daily Pulse email in Phase 12).
with daily as (
    select
        wiki,
        page_id,
        cast(hour_start as date) as activity_date,
        cast(sum(edits) as bigint) as edits,
        cast(sum(reverts) as bigint) as reverts,
        cast(sum(bot_edits) as bigint) as bot_edits,
        max(distinct_editors) as peak_hour_editors,
        max(edits) as peak_hour_edits,
        arg_max(hour_start, edits) as peak_hour
    from {{ ref('stg_page_hourly') }}
    group by 1, 2, 3
),

ranked as (
    select
        *,
        row_number() over (
            partition by wiki, activity_date order by edits desc, page_id
        ) as rank_in_day
    from daily
)

select
    r.wiki,
    r.activity_date,
    r.rank_in_day,
    r.page_id,
    p.page_title,
    r.edits,
    r.reverts,
    r.bot_edits,
    r.peak_hour,
    r.peak_hour_edits,
    r.peak_hour_editors
from ranked r
join {{ ref('dim_pages') }} p using (wiki, page_id)
where r.rank_in_day <= 25
