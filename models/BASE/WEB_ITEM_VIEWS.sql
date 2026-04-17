select
    add_to_cart_quantity,
    item_name,
    item_view_at,
    price_per_unit,
    remove_from_cart_quantity,
    session_id,
    _fivetran_deleted,
    _fivetran_id,
    _fivetran_synced
from {{ source("web_schema", "ITEM_VIEWS") }}
