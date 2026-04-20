with returns as (

    select
        order_id,
        max(case when is_refunded = 'yes' then 1 else 0 end) as is_refunded_flag,
        min(returned_at) as returned_at
    from {{ ref('GOOGLEDRIVE_RETURNS') }}
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
    coalesce(r.is_refunded_flag, 0) as is_refunded_flag,
    r.returned_at
from {{ ref('int_orders') }} o
left join returns r
    on o.order_id = r.order_id