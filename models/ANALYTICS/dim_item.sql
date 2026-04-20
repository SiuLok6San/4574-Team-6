select
    item_name,
    price_per_unit
from {{ ref('int_items') }}