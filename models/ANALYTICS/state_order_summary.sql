select
    state,
    count(*) as total_orders,
    count(distinct session_id) as total_sessions_with_orders,
    avg(coalesce(shipping_cost, 0)) as avg_shipping_cost,
    avg(coalesce(tax_rate, 0)) as avg_tax_rate,
    sum(coalesce(item_revenue, 0)) as total_item_revenue,
    sum(coalesce(order_revenue, 0)) as total_order_revenue,
    sum(coalesce(total_revenue, 0)) as total_revenue,
    avg(coalesce(order_revenue, 0)) as avg_order_revenue,
    avg(coalesce(total_revenue, 0)) as avg_total_revenue
from {{ ref('fact_order') }}
where state is not null
group by 1
order by total_orders desc