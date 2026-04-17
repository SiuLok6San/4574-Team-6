select
    RETURNED_AT,
    ORDER_ID,
    IS_REFUNDED
from {{ source('google_drive', 'RETURNS') }}