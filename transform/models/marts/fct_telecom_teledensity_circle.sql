-- Circle-level teledensity (wireline segment - the reliably parsed QPIR table).
{{ config(materialized='table') }}

select
    period_date,
    service_area,
    rural_pct,
    urban_pct,
    total_pct
from {{ ref('stg_trai_teledensity_circle') }}
