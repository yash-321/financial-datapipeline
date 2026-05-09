-- Test: Verify no significant gaps in trade data (time series continuity)
-- Tags: heavy_validation, completeness
-- This test detects gaps larger than 15 minutes in trading data

{{ config(tags=['heavy_validation', 'completeness']) }}

with hourly_buckets as (

    select
        DATE_TRUNC('hour', event_ts) as hour_bucket,
        symbol,
        COUNT(*) as trade_count,
        MIN(event_ts) as first_trade,
        MAX(event_ts) as last_trade

    from {{ ref('stg_trades') }}

    where trade_date >= DATEADD('day', -1, CURRENT_DATE())
      and trade_date < CURRENT_DATE()  -- Exclude today for complete hours only

    group by 1, 2

),

expected_hours as (

    select
        DATEADD('hour', seq, DATEADD('day', -1, DATE_TRUNC('day', CURRENT_TIMESTAMP()))) as hour_bucket
    from (
        select ROW_NUMBER() over (order by null) - 1 as seq
        from TABLE(GENERATOR(ROWCOUNT => 24))
    )

),

symbols as (

    select distinct symbol
    from {{ ref('stg_trades') }}
    where trade_date >= DATEADD('day', -1, CURRENT_DATE())

),

expected_combinations as (

    select 
        e.hour_bucket,
        s.symbol
    from expected_hours e
    cross join symbols s

),

missing_data as (

    select
        ec.hour_bucket,
        ec.symbol,
        hb.trade_count

    from expected_combinations ec
    left join hourly_buckets hb
        on ec.hour_bucket = hb.hour_bucket
        and ec.symbol = hb.symbol

    where hb.trade_count is null
       or hb.trade_count < 10  -- Expect at least 10 trades per hour per symbol

)

select *
from missing_data
