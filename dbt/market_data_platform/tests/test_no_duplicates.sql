-- Test: Verify no duplicate trade_ids after deduplication
-- Tags: incremental_validation, consistency
-- Fast test for incremental runs

{{ config(tags=['incremental_validation', 'consistency']) }}

with duplicates as (

    select
        trade_id,
        COUNT(*) as occurrence_count

    from {{ ref('stg_trades') }}

    where trade_date >= DATEADD('day', -1, CURRENT_DATE())

    group by trade_id
    having COUNT(*) > 1

)

select *
from duplicates
