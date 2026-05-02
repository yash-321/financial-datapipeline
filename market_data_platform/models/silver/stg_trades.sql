{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trade_id'
    )
}}

with filtered_bronze as (

    select *
    from {{ source('DATAPIPELINE', 'RAW_TRADES') }}

    {% if is_incremental() %}
        where event_timestamp >= (
            select coalesce(max(event_timestamp), 0) 
            from {{ this }}
        ) - 600000 -- 10 minutes buffer to account for late arriving data
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
    cast(CURRENT_TIMESTAMP() as timestamp_ntz) AS processed_at
from deduplicated_data

