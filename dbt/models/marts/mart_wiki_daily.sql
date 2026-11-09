-- Daily activity per wiki: volume, who edits, how much is reverted.
with hourly as (
    select
        wiki,
        cast(hour_start as date) as activity_date,
        cast(sum(edits) as bigint) as edits,
        cast(sum(new_pages) as bigint) as new_pages,
        cast(sum(bot_edits) as bigint) as bot_edits,
        cast(sum(anon_edits) as bigint) as anon_edits,
        cast(sum(reverts) as bigint) as reverts,
        cast(sum(bytes_delta) as bigint) as bytes_delta,
        count(*) as hours_with_data
    from {{ ref('stg_wiki_hourly') }}
    group by 1, 2
),

distincts as (
    select
        wiki,
        event_date as activity_date,
        count(distinct page_id) as active_pages,
        count(distinct coalesce(user_name, 'hidden:' || event_id)) as active_editors
    from {{ ref('stg_edits') }}
    group by 1, 2
)

select
    h.wiki,
    h.activity_date,
    h.edits,
    h.new_pages,
    d.active_pages,
    d.active_editors,
    h.bot_edits,
    h.anon_edits,
    h.reverts,
    round(h.bot_edits / h.edits, 4) as bot_share,
    round(h.anon_edits / h.edits, 4) as anon_share,
    round(h.reverts / h.edits, 4) as revert_rate,
    h.bytes_delta,
    h.hours_with_data
from hourly h
join distincts d using (wiki, activity_date)
