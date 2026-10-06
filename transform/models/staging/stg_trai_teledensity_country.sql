-- Staging: TRAI country wireless teledensity (silver).
select
    period_date,
    wireless_teledensity_pct
from read_parquet('data/silver/trai/teledensity_country.parquet')
