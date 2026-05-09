-- Test: Verify price values are within reasonable bounds
-- Tags: heavy_validation
-- This test catches potential data corruption or anomalies in price data

{{ config(tags=['heavy_validation']) }}

with price_stats as (

    select
        symbol,
        trade_date,
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as avg_price,
        STDDEV(price) as price_stddev,
        COUNT(*) as trade_count

    from {{ ref('stg_trades') }}

    where trade_date >= DATEADD('day', -1, CURRENT_DATE())

    group by 1, 2

),

anomalies as (

    select
        symbol,
        trade_date,
        min_price,
        max_price,
        avg_price,
        price_stddev,
        trade_count,
        -- Flag various anomaly conditions
        CASE
            WHEN min_price <= 0 THEN 'NEGATIVE_OR_ZERO_PRICE'
            WHEN max_price > 10000 THEN 'EXTREME_HIGH_PRICE'
            WHEN (max_price - min_price) / NULLIF(avg_price, 0) > 0.5 THEN 'HIGH_VOLATILITY'
            WHEN price_stddev / NULLIF(avg_price, 0) > 0.2 THEN 'ABNORMAL_STDDEV'
        END as anomaly_type

    from price_stats

    where min_price <= 0
       or max_price > 10000
       or (max_price - min_price) / NULLIF(avg_price, 0) > 0.5
       or price_stddev / NULLIF(avg_price, 0) > 0.2

)

select *
from anomalies
