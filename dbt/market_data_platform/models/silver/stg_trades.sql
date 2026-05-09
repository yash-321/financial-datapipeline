{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trade_id'
    )
}}

{# Backfill configuration - defaults to incremental behavior #}
{% set backfill_hours = var('backfill_hours', none) %}

with filtered_bronze as (

    select *
    from {{ source('DATAPIPELINE', 'RAW_TRADES') }}

    {% if is_incremental() and backfill_hours is none %}
        -- Standard incremental: get new records since last run with 10-minute buffer
        where event_timestamp >= (
            select coalesce(max(event_timestamp), 0) 
            from {{ this }}
        ) - 600000 -- 10 minutes buffer to account for late arriving data
    {% elif backfill_hours is not none %}
        -- Backfill mode: process last N hours of data
        where event_timestamp >= DATEDIFF('millisecond', '1970-01-01'::timestamp_ntz, 
                                          DATEADD('hour', -{{ backfill_hours }}, CURRENT_TIMESTAMP()))
    {% endif %}

),

deduplicated_data as (

    select *
    from filtered_bronze

    qualify row_number() over (
        partition by trade_id
        order by event_timestamp desc, ingestion_timestamp desc
    ) = 1

)


select
    trade_id,
    symbol,
    price,
    quantity,
    event_timestamp,
    ingestion_timestamp,
    TO_TIMESTAMP_NTZ(event_timestamp / 1000) AS event_ts,
    DATE(TO_TIMESTAMP_NTZ(event_timestamp / 1000)) AS trade_date,
    CURRENT_TIMESTAMP()::timestamp_ntz AS processed_at
from deduplicated_data

