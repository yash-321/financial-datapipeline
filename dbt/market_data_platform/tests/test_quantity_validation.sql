-- Test: Verify quantity values are valid
-- Tags: heavy_validation
-- This test ensures trade quantities are positive and reasonable

{{ config(tags=['heavy_validation']) }}

with invalid_quantities as (

    select
        trade_id,
        symbol,
        quantity,
        price,
        event_ts,
        trade_date,
        CASE
            WHEN quantity <= 0 THEN 'NON_POSITIVE_QUANTITY'
            WHEN quantity > 1000000 THEN 'EXTREME_QUANTITY'
            WHEN quantity != FLOOR(quantity) THEN 'FRACTIONAL_QUANTITY'
        END as issue_type

    from {{ ref('stg_trades') }}

    where trade_date >= DATEADD('day', -1, CURRENT_DATE())
      and (
          quantity <= 0
          or quantity > 1000000
      )

)

select *
from invalid_quantities
