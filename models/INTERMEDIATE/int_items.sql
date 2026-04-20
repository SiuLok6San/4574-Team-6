select distinct
    item_name,
    price_per_unit
from {{ ref('WEB_ITEM_VIEWS') }}
where item_name is not null