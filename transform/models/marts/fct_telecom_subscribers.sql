-- TRAI subscriber base by circle x operator x month with growth and market share.
{{ config(materialized='table') }}

with subscribers as (
    select
        period_month,
        segment,
        service_area,
        operator,
        subscribers
    from {{ ref('stg_trai_subscribers') }}
),

growth as (
    select
        *,
        lag(subscribers) over (
            partition by segment, service_area, operator order by period_month
        ) as prev_subscribers
    from subscribers
),

circle_totals as (
    select
        period_month,
        segment,
        service_area,
        sum(subscribers) as circle_total
    from subscribers
    group by period_month, segment, service_area
)

select
    g.period_month,
    g.segment,
    g.service_area,
    g.operator,
    g.subscribers,
    case
        when g.prev_subscribers is null or g.prev_subscribers = 0 then null
        else 100.0 * (g.subscribers - g.prev_subscribers) / g.prev_subscribers
    end as subscribers_mom_growth_pct,
    100.0 * g.subscribers / nullif(t.circle_total, 0) as circle_share_pct,
    t.circle_total
from growth g
left join circle_totals t
    on t.period_month = g.period_month
   and t.segment = g.segment
   and t.service_area = g.service_area
