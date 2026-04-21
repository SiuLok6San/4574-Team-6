select
    add_to_cart_quantity,
    item_name,
    item_view_at,
    price_per_unit,
    remove_from_cart_quantity,
    session_id,
<<<<<<< HEAD
    "_fivetran_deleted",
    "_fivetran_id",
    "_fivetran_synced"
=======
    "_fivetran_deleted" as _fivetran_deleted,
    "_fivetran_id" as _fivetran_id,
    "_fivetran_synced" as _fivetran_synced
>>>>>>> dd294406af7b40420bfffad475cd3bfb05c0d8a0
from {{ source("web_schema", "ITEM_VIEWS") }}