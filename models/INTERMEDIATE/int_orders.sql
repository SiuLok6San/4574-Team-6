with ranked_orders as (

    select
        *,
        row_number() over (
            partition by order_id
            order by order_at
        ) as rn
    from {{ ref('WEB_ORDERS') }}

),

orders as (

    select
        order_id,
        session_id,
        order_at,
        client_name,
        payment_method,
        shipping_address,
        try_to_number(replace(replace(shipping_cost, 'USD ', ''), ',', '')) as shipping_cost,
        phone,
        state,
        tax_rate
    from ranked_orders
    where rn = 1

),

item_revenue_by_session as (

    select
        session_id,
        sum(coalesce(price_per_unit, 0) * coalesce(add_to_cart_quantity, 0)) as item_revenue
    from {{ ref('WEB_ITEM_VIEWS') }}
    group by 1

)

select
    o.order_id,
    o.session_id,
    o.order_at,
    o.client_name,
    o.payment_method,
    o.shipping_address,
    o.shipping_cost,
    o.phone,
    o.state,
    o.tax_rate,
    coalesce(i.item_revenue, 0) as item_revenue,
    coalesce(i.item_revenue, 0) + coalesce(o.shipping_cost, 0) as order_revenue,
    (coalesce(i.item_revenue, 0) + coalesce(o.shipping_cost, 0)) * (1 + coalesce(o.tax_rate, 0)) as total_revenue
from orders o
left join item_revenue_by_session i
    on o.session_id = i.session_id