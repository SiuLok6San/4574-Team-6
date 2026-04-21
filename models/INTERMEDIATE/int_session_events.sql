with page_views as (

    select
        session_id,
        max(case when page_name = 'landing_page' then 1 else 0 end) as viewed_landing_page,
        max(case when page_name = 'shop_plants' then 1 else 0 end) as viewed_shop_plants,
        max(case when page_name = 'cart' then 1 else 0 end) as viewed_cart,
        min(view_at) as first_page_view_at
    from {{ ref('WEB_PAGE_VIEWS') }}
    where session_id is not null
    group by 1

),

item_views as (

    select
        session_id,
        max(1) as viewed_item,
        sum(coalesce(add_to_cart_quantity, 0)) as total_add_to_cart_qty,
        sum(coalesce(remove_from_cart_quantity, 0)) as total_remove_from_cart_qty
    from {{ ref('WEB_ITEM_VIEWS') }}
    where session_id is not null
    group by 1

),

orders as (

    select
        session_id,
        max(1) as placed_order,
        count(*) as num_orders
    from {{ ref('int_orders') }}
    where session_id is not null
    group by 1

),

all_sessions as (

    select session_id from page_views
    union
    select session_id from item_views
    union
    select session_id from orders

)

select
    s.session_id,
    coalesce(p.viewed_landing_page, 0) as viewed_landing_page,
    coalesce(p.viewed_shop_plants, 0) as viewed_shop_plants,
    coalesce(p.viewed_cart, 0) as viewed_cart,
    coalesce(i.viewed_item, 0) as viewed_item,
    coalesce(i.total_add_to_cart_qty, 0) as total_add_to_cart_qty,
    coalesce(i.total_remove_from_cart_qty, 0) as total_remove_from_cart_qty,
    coalesce(o.placed_order, 0) as placed_order,
    coalesce(o.num_orders, 0) as num_orders,
    p.first_page_view_at
from all_sessions s
left join page_views p
    on s.session_id = p.session_id
left join item_views i
    on s.session_id = i.session_id
left join orders o
    on s.session_id = o.session_id