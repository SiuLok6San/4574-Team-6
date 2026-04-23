select
    date_day,
    sessions_count,
    page_views_count,
    item_views_count,
    total_orders,
    returns_count,
    case
        when sessions_count = 0 then null
        else total_orders::float / sessions_count
    end as session_to_order_rate,
    case
        when total_orders = 0 then null
        else returns_count::float / total_orders
    end as return_rate
from {{ ref('int_daily_web_metrics') }}
order by 1