select
    order_id,
    returned_at,
    case
        when is_refunded = 'yes' then 1
        else 0
    end as is_refunded_flag
from {{ ref('GOOGLEDRIVE_RETURNS') }}