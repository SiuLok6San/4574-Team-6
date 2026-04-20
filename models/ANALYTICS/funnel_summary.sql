with session_funnel as (

    select
        session_id,
        viewed_landing_page,
        viewed_shop_plants,
        viewed_cart,
        viewed_item,
        case
            when total_add_to_cart_qty > 0 then 1
            else 0
        end as added_to_cart,
        placed_order
    from {{ ref('fact_session') }}

)

select
    count(distinct session_id) as total_sessions,
    sum(case when viewed_landing_page = 1 then 1 else 0 end) as sessions_viewed_landing_page,
    sum(case when viewed_shop_plants = 1 then 1 else 0 end) as sessions_viewed_shop_plants,
    sum(case when viewed_item = 1 then 1 else 0 end) as sessions_viewed_item,
    sum(case when viewed_cart = 1 then 1 else 0 end) as sessions_viewed_cart,
    sum(case when added_to_cart = 1 then 1 else 0 end) as sessions_added_to_cart,
    sum(case when placed_order = 1 then 1 else 0 end) as sessions_placed_order
from session_funnel