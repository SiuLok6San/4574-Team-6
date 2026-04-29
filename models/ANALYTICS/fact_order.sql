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
    first_returned_at,
    refunded_flag,
    return_record_count,
    item_revenue,
    order_revenue,
    total_revenue
from {{ ref('int_order_returns') }}