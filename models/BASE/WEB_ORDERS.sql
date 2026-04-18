select
    client_name,
    order_at,
    order_id,
    payment_info,
    payment_method,
    phone,
    session_id,
    shipping_address,
    shipping_cost,
    state,
    tax_rate,
    "_fivetran_deleted" as _fivetran_deleted,
    "_fivetran_id" as _fivetran_id,
    "_fivetran_synced" as _fivetran_synced
from {{ source('web_schema', 'ORDERS') }}