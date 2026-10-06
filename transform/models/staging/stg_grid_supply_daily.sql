-- Staging: Grid-India daily power supply position, metrics pivoted to columns (silver).
select
    report_date,
    region_code,
    region_name,
    max(case when metric = 'demand_met_evening_peak_mw' then value end) as demand_met_evening_peak_mw,
    max(case when metric = 'shortage_evening_peak_mw' then value end) as shortage_evening_peak_mw,
    max(case when metric = 'energy_met_mu' then value end) as energy_met_mu
from read_parquet('data/silver/gridindia/supply_daily.parquet')
group by report_date, region_code, region_name
