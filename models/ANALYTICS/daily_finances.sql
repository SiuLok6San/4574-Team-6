with expenses as (

    select
        expense_date as date_day,
        sum(expense_amount) as total_expenses
    from {{ ref('GOOGLEDRIVE_EXPENSES') }}
    group by 1

),

orders as (

    select
        cast(order_at as date) as date_day,
        count(*) as total_orders,
        sum(coalesce(shipping_cost, 0)) as shipping_revenue,
        sum(coalesce(item_revenue, 0)) as total_item_revenue,
        sum(coalesce(order_revenue, 0)) as total_order_revenue,
        sum(coalesce(total_revenue, 0)) as total_revenue
    from {{ ref('fact_order') }}
    group by 1

),

returns as (

    select
        first_returned_at as date_day,
        count(*) as total_returns,
        sum(case when refunded_flag = 1 then 1 else 0 end) as total_refunded_orders
    from {{ ref('fact_order') }}
    where first_returned_at is not null
    group by 1

)

select
    coalesce(e.date_day, o.date_day, r.date_day) as date_day,
    coalesce(e.total_expenses, 0) as total_expenses,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.shipping_revenue, 0) as shipping_revenue,
    coalesce(o.total_item_revenue, 0) as total_item_revenue,
    coalesce(o.total_order_revenue, 0) as total_order_revenue,
    coalesce(o.total_revenue, 0) as total_revenue,
    coalesce(r.total_returns, 0) as total_returns,
    coalesce(r.total_refunded_orders, 0) as total_refunded_orders
from expenses e
full outer join orders o
    on e.date_day = o.date_day
full outer join returns r
    on coalesce(e.date_day, o.date_day) = r.date_day