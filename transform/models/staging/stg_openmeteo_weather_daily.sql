-- Staging: Open-Meteo daily weather, archive observations only (silver).
select
    city,
    latitude,
    longitude,
    date,
    temperature_2m_max,
    temperature_2m_min,
    precipitation_sum,
    wind_speed_10m_max
from read_parquet('data/silver/openmeteo/weather_daily.parquet')
