select
    s.session_id,
    s.client_id,
    s.session_at,
    s.os,
    s.ip,
    coalesce(e.viewed_landing_page, 0) as viewed_landing_page,
    coalesce(e.viewed_shop_plants, 0) as viewed_shop_plants,
    coalesce(e.viewed_cart, 0) as viewed_cart,
    coalesce(e.viewed_item, 0) as viewed_item,
    coalesce(e.total_add_to_cart_qty, 0) as total_add_to_cart_qty,
    coalesce(e.total_remove_from_cart_qty, 0) as total_remove_from_cart_qty,
    coalesce(e.placed_order, 0) as placed_order,
    coalesce(e.num_orders, 0) as num_orders
from {{ ref('int_sessions') }} s
left join {{ ref('int_session_events') }} e
    on s.session_id = e.session_id