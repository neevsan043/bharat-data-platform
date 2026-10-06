-- Date dimension covering the platform's analytical range.
{{ config(materialized='table') }}

with spine as (
    select
        cast(d as date) as date_day
    from generate_series(date '2021-06-01', current_date, interval 1 day) as t(d)
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    strftime(date_day, '%B') as month_name,
    strftime(date_day, '%Y-%m') as year_month,
    cast(strftime(date_day, '%W') as integer) as week_of_year,
    case when date_day = date_trunc('month', date_day) then true else false end as is_month_start
from spine
