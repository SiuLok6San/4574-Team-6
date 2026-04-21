-- Aggregates order metrics by day from web orders.
with orders as (

    select
        order_id,
        session_id,
        order_at,
        shipping_cost,
        state,
        tax_rate
    from {{ ref('int_orders') }}

)

select
    cast(order_at as date) as date_day,
    count(*) as total_orders,
    count(distinct session_id) as unique_session_count,
    count(distinct state) as states_count,
    sum(coalesce(shipping_cost, 0)) as total_shipping_revenue,
    avg(coalesce(tax_rate, 0)) as avg_tax_rate
from orders
where order_at is not null
group by 1
order by 1