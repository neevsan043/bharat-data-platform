-- Staging: World Bank India indicators (silver).
select
    indicator_id,
    indicator_name,
    country_iso3,
    year,
    value
from read_parquet('data/silver/worldbank/indicators.parquet')
