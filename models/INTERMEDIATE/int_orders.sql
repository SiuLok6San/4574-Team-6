with ranked as (

    select
        *,
        row_number() over (
            partition by order_id
            order by order_at
        ) as rn
    from {{ ref('WEB_ORDERS') }}

)

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
from ranked
where rn = 1