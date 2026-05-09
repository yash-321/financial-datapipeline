-- Test: Verify data freshness for incremental runs
-- Tags: incremental_validation
-- Ensures recent data is being processed

{{ config(tags=['incremental_validation']) }}

with freshness_check as (

    select
        MAX(event_ts) as latest_event,
        DATEDIFF('minute', MAX(event_ts), CURRENT_TIMESTAMP()) as staleness_minutes

    from {{ ref('stg_trades') }}

)

select *
from freshness_check
where staleness_minutes > 15  -- Alert if data is more than 15 minutes stale
