with ranked as (

    select
        trade_id,
        event_timestamp,
        ingestion_timestamp,
        row_number() over (
            partition by trade_id
            order by event_timestamp desc, ingestion_timestamp desc
        ) as rn

    from {{ source('DATAPIPELINE', 'RAW_TRADES') }}

),

latest as (
    select *
    from ranked
    where rn = 1
),

mismatches as (

    select s.trade_id
    from {{ ref('stg_trades') }} s
    join latest l
      on s.trade_id = l.trade_id

    where
        s.event_timestamp != l.event_timestamp
        or s.ingestion_timestamp != l.ingestion_timestamp

)

select *
from mismatches