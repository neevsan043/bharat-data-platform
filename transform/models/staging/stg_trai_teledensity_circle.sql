-- Staging: TRAI circle teledensity (wireline; the only circle-level teledensity
-- table whose QPIR layout parses reliably - see docs/bronze-report.md).
select
    period_date,
    service_area,
    rural_pct,
    urban_pct,
    total_pct
from read_parquet('data/silver/trai/teledensity_circle.parquet')
