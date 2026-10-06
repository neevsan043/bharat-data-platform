-- Business rule: national wireless subscriber base per month within a sane band.
-- India's wireless base is ~1.1-1.2 billion; summing operator rows per circle must
-- land near that. Catches dropped circles or operator duplication.
select
    period_month,
    sum(subscribers) as wireless_total
from {{ ref('fct_telecom_subscribers') }}
where segment = 'wireless'
group by period_month
having sum(subscribers) < 900000000
    or sum(subscribers) > 1400000000
