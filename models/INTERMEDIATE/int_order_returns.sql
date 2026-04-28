with orders as (

    select
        order_id,
        session_id,
        order_at,
        client_name,
        payment_method,
        shipping_address,
        shipping_cost,
        phone,
        state,
        tax_rate,
        item_revenue,  
        order_revenue,  
        total_revenue   
    from {{ ref('int_orders') }}

),

returns as (

    select
        order_id,
        min(returned_at) as first_returned_at,
        max(case when is_refunded = 'yes' then 1 else 0 end) as refunded_flag,
        count(*) as return_record_count
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
    o.item_revenue,
    o.order_revenue,    
    o.total_revenue,    
    r.first_returned_at,
    coalesce(r.refunded_flag, 0) as refunded_flag,
    coalesce(r.return_record_count, 0) as return_record_count
from orders o
left join returns r
    on o.order_id = r.order_id