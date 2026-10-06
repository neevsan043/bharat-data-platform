-- Business rule: India's GDP (current US$) must be plausible for recent years.
-- 2025 value ~3.96 trillion USD (verified live during Phase 0); this catches
-- unit corruption or a mis-joined indicator series.
select indicator_id, year, value
from {{ ref('fct_macro_indicators') }}
where indicator_id = 'NY.GDP.MKTP.CD'
  and year >= 2020
  and (value < 2e12 or value > 6e12)
