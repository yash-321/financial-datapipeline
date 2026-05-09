-- Test: Verify all expected symbols are present in the data
-- Tags: heavy_validation, completeness
-- This test ensures we're receiving data for all expected trading symbols

{{ config(tags=['heavy_validation', 'completeness']) }}

{% set expected_symbols = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'TSLA'] %}

with expected as (

    {% for symbol in expected_symbols %}
    select '{{ symbol }}' as symbol
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

actual_symbols as (

    select distinct symbol
    from {{ ref('stg_trades') }}
    where trade_date >= DATEADD('day', -1, CURRENT_DATE())

),

missing_symbols as (

    select e.symbol as missing_symbol
    from expected e
    left join actual_symbols a
        on e.symbol = a.symbol
    where a.symbol is null

)

select *
from missing_symbols
