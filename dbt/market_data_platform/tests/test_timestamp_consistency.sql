-- Test: Verify timestamp consistency
-- Tags: heavy_validation, consistency
-- Ensures event_timestamp and ingestion_timestamp are logically consistent

{{ config(tags=['heavy_validation', 'consistency']) }}

with timestamp_issues as (

    select
        trade_id,
        symbol,
        event_timestamp,
        ingestion_timestamp,
        event_ts,
        trade_date,
        processed_at,
        CASE
            WHEN ingestion_timestamp < event_timestamp THEN 'INGESTION_BEFORE_EVENT'
            WHEN event_timestamp > DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, CURRENT_TIMESTAMP()) THEN 'FUTURE_EVENT'
            WHEN ingestion_timestamp > DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, CURRENT_TIMESTAMP()) THEN 'FUTURE_INGESTION'
            WHEN (ingestion_timestamp - event_timestamp) > 3600000 THEN 'LATE_INGESTION'  -- More than 1 hour delay
        END as issue_type

    from {{ ref('stg_trades') }}

    where trade_date >= DATEADD('day', -1, CURRENT_DATE())
      and (
          ingestion_timestamp < event_timestamp
          or event_timestamp > DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, CURRENT_TIMESTAMP())
          or ingestion_timestamp > DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, CURRENT_TIMESTAMP())
          or (ingestion_timestamp - event_timestamp) > 3600000
      )

)

select *
from timestamp_issues
