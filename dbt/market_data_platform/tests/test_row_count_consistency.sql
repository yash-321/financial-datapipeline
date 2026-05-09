-- Test: Verify row counts match between bronze and silver (after deduplication)
-- Tags: heavy_validation, consistency
-- This test ensures no data is lost during transformation

{{ config(tags=['heavy_validation', 'consistency']) }}

with bronze_counts as (

    select
        DATE(TO_TIMESTAMP_NTZ(event_timestamp / 1000)) as trade_date,
        symbol,
        COUNT(DISTINCT trade_id) as bronze_unique_trades

    from {{ source('DATAPIPELINE', 'RAW_TRADES') }}

    where event_timestamp >= DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, 
                                      DATEADD('hour', -24, CURRENT_TIMESTAMP()))

    group by 1, 2

),

silver_counts as (

    select
        trade_date,
        symbol,
        COUNT(*) as silver_trades

    from {{ ref('stg_trades') }}

    where event_timestamp >= DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, 
                                      DATEADD('hour', -24, CURRENT_TIMESTAMP()))

    group by 1, 2

),

mismatches as (

    select
        COALESCE(b.trade_date, s.trade_date) as trade_date,
        COALESCE(b.symbol, s.symbol) as symbol,
        COALESCE(b.bronze_unique_trades, 0) as bronze_count,
        COALESCE(s.silver_trades, 0) as silver_count,
        COALESCE(b.bronze_unique_trades, 0) - COALESCE(s.silver_trades, 0) as difference

    from bronze_counts b
    full outer join silver_counts s
        on b.trade_date = s.trade_date
        and b.symbol = s.symbol

    where COALESCE(b.bronze_unique_trades, 0) != COALESCE(s.silver_trades, 0)

)

select *
from mismatches
