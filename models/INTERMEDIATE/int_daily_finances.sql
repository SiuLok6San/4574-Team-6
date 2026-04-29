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
        sum(coalesce(shipping_cost, 0)) as shipping_revenue
    from {{ ref('int_orders') }}
    group by 1

),

revenue as (

    select
        cast(order_at as date) as date_day,
        sum(coalesce(item_revenue, 0)) as total_item_revenue,
        sum(coalesce(order_revenue, 0)) as total_order_revenue,
        sum(coalesce(total_revenue, 0)) as gross_revenue
    from {{ ref('fact_order') }}
    group by 1

),

returns as (

    select
        returned_at as date_day,
        count(*) as total_returns,
        sum(case when is_refunded = 'yes' then 1 else 0 end) as total_refunded_orders
    from {{ ref('GOOGLEDRIVE_RETURNS') }}
    group by 1

),

refunds as (

    select
        cast(first_returned_at as date) as date_day,
        count(distinct order_id) as refunded_orders_from_fact,
        sum(coalesce(total_revenue, 0)) as refund_revenue
    from {{ ref('fact_order') }}
    where refunded_flag = 1
    group by 1

)

select
    coalesce(e.date_day, o.date_day, rev.date_day, r.date_day, f.date_day) as date_day,

    coalesce(e.total_expenses, 0) as total_expenses,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.shipping_revenue, 0) as shipping_revenue,

    coalesce(rev.total_item_revenue, 0) as total_item_revenue,
    coalesce(rev.total_order_revenue, 0) as total_order_revenue,

    coalesce(rev.gross_revenue, 0) as gross_revenue,
    coalesce(f.refund_revenue, 0) as refund_revenue,
    coalesce(rev.gross_revenue, 0) - coalesce(f.refund_revenue, 0) as net_revenue,

    coalesce(rev.gross_revenue, 0) - coalesce(f.refund_revenue, 0) - coalesce(e.total_expenses, 0) as net_profit,

    coalesce(r.total_returns, 0) as total_returns,
    coalesce(r.total_refunded_orders, 0) as total_refunded_orders

from expenses e
full outer join orders o
    on e.date_day = o.date_day
full outer join revenue rev
    on coalesce(e.date_day, o.date_day) = rev.date_day
full outer join returns r
    on coalesce(e.date_day, o.date_day, rev.date_day) = r.date_day
full outer join refunds f
    on coalesce(e.date_day, o.date_day, rev.date_day, r.date_day) = f.date_day