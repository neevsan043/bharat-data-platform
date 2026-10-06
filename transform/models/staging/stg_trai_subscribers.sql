-- Staging: TRAI subscriber base by service area x operator x month (silver).
select
    period_month,
    segment,
    service_area,
    operator,
    subscribers
from read_parquet('data/silver/trai/subscribers.parquet')
