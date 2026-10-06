-- Business rule: month-over-month UPI volume growth within +-60%.
-- The network grows a few percent monthly; a larger swing means a broken row
-- (partial export slipping through, or a units error).
select
    month_start,
    volume_mom_growth_pct
from {{ ref('fct_upi_monthly') }}
where not is_partial_month
  and volume_mom_growth_pct is not null
  and abs(volume_mom_growth_pct) > 60
