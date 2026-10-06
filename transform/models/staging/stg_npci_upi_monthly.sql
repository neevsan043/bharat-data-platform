-- Staging: NPCI UPI monthly product statistics (silver).
select
    month_start,
    fy,
    volume_mn,
    avg_daily_volume_mn,
    value_cr,
    avg_daily_value_cr,
    is_partial_month
from read_parquet('data/silver/npci/upi_monthly.parquet')
