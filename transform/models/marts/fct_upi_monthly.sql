-- UPI monthly product statistics with MoM/YoY growth.
-- Growth is suppressed when either month is a flagged partial export.
-- Bank-wise concentration is intentionally absent: NPCI's bank-wise statistics are
-- browser-gated (docs/data-sources.md) and no such drop has landed yet.
{{ config(materialized='table') }}

with monthly as (
    select
        month_start,
        volume_mn,
        value_cr,
        avg_daily_volume_mn,
        avg_daily_value_cr,
        is_partial_month
    from {{ ref('stg_npci_upi_monthly') }}
),

mom as (
    select
        *,
        lag(volume_mn) over (order by month_start) as prev_volume_mn,
        lag(value_cr) over (order by month_start) as prev_value_cr,
        lag(is_partial_month) over (order by month_start) as prev_is_partial
    from monthly
)

select
    month_start,
    volume_mn,
    value_cr,
    avg_daily_volume_mn,
    avg_daily_value_cr,
    value_cr * 10.0 / nullif(volume_mn, 0) as avg_ticket_size_inr,
    case
        when is_partial_month or prev_is_partial then null
        else 100.0 * (volume_mn - prev_volume_mn) / nullif(prev_volume_mn, 0)
    end as volume_mom_growth_pct,
    case
        when is_partial_month or prev_is_partial then null
        else 100.0 * (value_cr - prev_value_cr) / nullif(prev_value_cr, 0)
    end as value_mom_growth_pct,
    100.0 * (volume_mn - lag(volume_mn, 12) over (order by month_start))
        / nullif(lag(volume_mn, 12) over (order by month_start), 0) as volume_yoy_growth_pct,
    100.0 * (value_cr - lag(value_cr, 12) over (order by month_start))
        / nullif(lag(value_cr, 12) over (order by month_start), 0) as value_yoy_growth_pct,
    is_partial_month
from mom
