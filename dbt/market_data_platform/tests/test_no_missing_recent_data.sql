select *
from {{ source('DATAPIPELINE', 'RAW_TRADES') }} b
left join {{ ref('stg_trades') }} s
  on b.trade_id = s.trade_id

where
    b.event_timestamp >= (
        select max(event_timestamp) - 600000
        from {{ source('DATAPIPELINE', 'RAW_TRADES') }}
    )
    and s.trade_id is null