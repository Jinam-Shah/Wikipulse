select * from {{ ref('stg_page_hourly') }} where reverts > edits or bot_edits > edits
