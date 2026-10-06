-- Business rule: implied UPI ticket size (value per transaction) must sit in a
-- plausible band. Historically ~INR 1,200-2,000 for these months; a broken unit
-- conversion (Cr vs Mn) would push this far outside.
select month_start, volume_mn, value_cr, avg_ticket_size_inr
from {{ ref('fct_upi_monthly') }}
where avg_ticket_size_inr is null
   or avg_ticket_size_inr < 100
   or avg_ticket_size_inr > 10000
