select
    date_day,
    total_expenses,
    total_orders,
    shipping_revenue,
    total_item_revenue,
    total_order_revenue,
    gross_revenue,
    refund_revenue,
    net_revenue,
    net_profit,
    total_returns,
    total_refunded_orders
from {{ ref('int_daily_finances') }}