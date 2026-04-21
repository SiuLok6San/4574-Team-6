select
    order_id,
    first_returned_at as returned_at,
    refunded_flag,
    return_record_count
from {{ ref('int_order_returns') }}
where return_record_count > 0