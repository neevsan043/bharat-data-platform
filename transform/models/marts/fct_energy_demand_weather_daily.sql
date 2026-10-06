-- Daily regional power demand joined with city weather aggregated to grid regions.
{{ config(materialized='table') }}

with city_region as (
    select geography_name as city, grid_region as region_name
    from {{ ref('dim_geography') }}
    where level = 'city'
),

weather_region as (
    select
        w.date as report_date,
        c.region_name,
        count(*) as cities_reported,
        avg(w.temperature_2m_max) as avg_temp_max_c,
        max(w.temperature_2m_max) as max_temp_max_c,
        avg(w.temperature_2m_min) as avg_temp_min_c,
        avg(w.precipitation_sum) as avg_precipitation_mm
    from {{ ref('stg_openmeteo_weather_daily') }} w
    join city_region c on c.city = w.city
    group by w.date, c.region_name
),

grid_region as (
    select
        report_date,
        region_name,
        max(demand_met_evening_peak_mw) as demand_met_evening_peak_mw,
        max(shortage_evening_peak_mw) as shortage_evening_peak_mw,
        max(energy_met_mu) as energy_met_mu
    from {{ ref('stg_grid_supply_daily') }}
    where region_code <> 'TOTAL'
    group by report_date, region_name
)

select
    g.report_date,
    g.region_name,
    g.demand_met_evening_peak_mw,
    g.shortage_evening_peak_mw,
    g.energy_met_mu,
    w.cities_reported,
    w.avg_temp_max_c,
    w.max_temp_max_c,
    w.avg_temp_min_c,
    w.avg_precipitation_mm
from grid_region g
left join weather_region w
    on w.report_date = g.report_date and w.region_name = g.region_name
