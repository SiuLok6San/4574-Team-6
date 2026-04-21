select
    item_name,
    min_price_per_unit,
    max_price_per_unit,
    price_variant_count,
    item_view_count,
    unique_session_count,
    total_add_to_cart_qty,
    total_remove_from_cart_qty
from {{ ref('int_item_metrics') }}