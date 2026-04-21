with item_views as (

    select
        item_name,
        price_per_unit,
        session_id,
        add_to_cart_quantity,
        remove_from_cart_quantity
    from {{ ref('WEB_ITEM_VIEWS') }}

)

select
    item_name,
    price_per_unit,
    count(*) as item_view_count,
    count(distinct session_id) as unique_session_count,
    sum(coalesce(add_to_cart_quantity, 0)) as total_add_to_cart_qty,
    sum(coalesce(remove_from_cart_quantity, 0)) as total_remove_from_cart_qty
from item_views
group by 1, 2