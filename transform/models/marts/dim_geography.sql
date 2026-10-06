-- Geography dimension: telecom circles, grid regions and weather cities.
-- The circle -> grid-region mapping is an analytical mapping defined by this
-- project (TRAI licensed service areas do not map 1:1 to grid regions).
{{ config(materialized='table') }}

with circles as (
    select distinct service_area as name
    from {{ ref('stg_trai_subscribers') }}
    union
    select distinct service_area from {{ ref('stg_trai_teledensity_circle') }}
),

circle_rows as (
    select
        'circle' as level,
        name as geography_name,
        case
            when name in ('Delhi', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir', 'Punjab',
                          'Rajasthan', 'UP (East)', 'UP (West)', 'U.P.(E)', 'U.P.(W)')
                then 'Northern'
            when name in ('Gujarat', 'Maharashtra', 'Mumbai', 'Madhya Pradesh', 'Goa')
                then 'Western'
            when name in ('Andhra Pradesh', 'Karnataka', 'Kerala', 'Tamil Nadu', 'Chennai', 'Telangana')
                then 'Southern'
            when name in ('Bihar', 'Kolkata', 'Odisha', 'West Bengal')
                then 'Eastern'
            when name in ('Assam', 'North East')
                then 'North-Eastern'
            when name = 'All India'
                then 'All India'
            else 'Unmapped'
        end as grid_region
    from circles
),

regions as (
    select distinct region_name as geography_name, region_name as grid_region
    from {{ ref('stg_grid_supply_daily') }}
),

cities as (
    select
        city as geography_name,
        case
            when city in ('delhi', 'jaipur', 'lucknow') then 'Northern'
            when city in ('mumbai', 'ahmedabad', 'pune') then 'Western'
            when city in ('chennai', 'bengaluru', 'hyderabad') then 'Southern'
            when city = 'kolkata' then 'Eastern'
            when city = 'guwahati' then 'North-Eastern'
        end as grid_region
    from {{ ref('stg_openmeteo_weather_daily') }}
)

select
    level || ':' || geography_name as geography_key,
    level,
    geography_name,
    coalesce(grid_region, 'Unmapped') as grid_region
from (
    select 'circle' as level, geography_name, grid_region from circle_rows
    union
    select 'region', geography_name, grid_region from regions
    union
    select 'city', geography_name, grid_region from cities
)
