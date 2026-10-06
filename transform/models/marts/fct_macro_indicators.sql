-- World Bank macro indicators with year-over-year growth.
{{ config(materialized='table') }}

with indicators as (
    select
        indicator_id,
        max(indicator_name) as indicator_name,
        year,
        max(value) as value
    from {{ ref('stg_worldbank_indicators') }}
    group by indicator_id, year
)

select
    indicator_id,
    indicator_name,
    year,
    value,
    100.0 * (value - lag(value) over (partition by indicator_id order by year))
        / nullif(abs(lag(value) over (partition by indicator_id order by year)), 0) as yoy_growth_pct
from indicators
