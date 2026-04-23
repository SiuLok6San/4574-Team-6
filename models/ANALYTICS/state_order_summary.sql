select
    state,
    count(*) as total_orders,
    count(distinct session_id) as total_sessions_with_orders,
    avg(coalesce(shipping_cost, 0)) as avg_shipping_cost,
    avg(coalesce(tax_rate, 0)) as avg_tax_rate
from {{ ref('fact_order') }}
where state is not null
group by 1
order by total_orders desc