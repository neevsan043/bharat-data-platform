-- Business rule: evening-peak shortage cannot exceed demand met.
-- Met + shortage approximates the requirement; shortage far above met would mean
-- misparsed columns (e.g. the report's own "Shoratge" typo row mixed up).
select report_date, region_name, demand_met_evening_peak_mw, shortage_evening_peak_mw
from {{ ref('fct_energy_demand_weather_daily') }}
where shortage_evening_peak_mw > demand_met_evening_peak_mw
