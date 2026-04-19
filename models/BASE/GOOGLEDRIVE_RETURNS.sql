select
    _file,
    _line,
    _modified as modified_ts,
    _fivetran_synced as fivetran_synced_ts,
    returned_at,
    trim(order_id) as order_id,
    case
        when lower(trim(is_refunded)) in ('true', 'yes', 'y', '1') then true
        when lower(trim(is_refunded)) in ('false', 'no', 'n', '0') then false
        else null
    end as is_refunded
from {{ source('google_drive', 'RETURNS') }}
