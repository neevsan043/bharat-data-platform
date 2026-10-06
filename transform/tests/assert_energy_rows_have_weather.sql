-- Business rule: every region-day in the energy mart must carry weather.
-- The weather network covers all five grid regions via mapped cities; a missing
-- join means the city-region map or the weather ingestion broke.
select report_date, region_name
from {{ ref('fct_energy_demand_weather_daily') }}
where avg_temp_max_c is null
  -- the ERA5 archive lags ~5 days behind real time; only assert on settled days
  and report_date < current_date - interval 7 days
