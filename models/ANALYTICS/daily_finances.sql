select
    date_day,
    total_expenses,
    total_orders,
    shipping_revenue,
    total_returns,
    total_refunded_orders
from {{ ref('int_daily_finances') }}