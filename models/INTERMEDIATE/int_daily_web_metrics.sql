-- Creates one consolidated daily web-performance table by joining sessions, page views, item views, orders, and returns.
with sessions as (

    select
        cast(session_at as date) as date_day,
        count(*) as sessions_count,
        count(distinct session_id) as distinct_sessions_count,
        count(distinct client_id) as distinct_client_count
    from {{ ref('int_sessions') }}
    where session_at is not null
    group by 1

),

engagement as (

    select *
    from {{ ref('int_dailypage_item_activity') }}

),

orders as (

    select *
    from {{ ref('int_daily_orders') }}

),

returns_data as (

    select
        cast(first_returned_at as date) as date_day,
        count(*) as returns_count,
        sum(coalesce(refunded_flag, 0)) as refunded_orders_count
    from {{ ref('int_order_returns') }}
    where first_returned_at is not null
    group by 1

)

select
    coalesce(s.date_day, e.date_day, o.date_day, r.date_day) as date_day,
    coalesce(s.sessions_count, 0) as sessions_count,
    coalesce(s.distinct_sessions_count, 0) as distinct_sessions_count,
    coalesce(s.distinct_client_count, 0) as distinct_client_count,
    coalesce(e.page_views_count, 0) as page_views_count,
    coalesce(e.page_view_sessions_count, 0) as page_view_sessions_count,
    coalesce(e.item_views_count, 0) as item_views_count,
    coalesce(e.item_view_sessions_count, 0) as item_view_sessions_count,
    coalesce(e.total_add_to_cart_qty, 0) as total_add_to_cart_qty,
    coalesce(e.total_remove_from_cart_qty, 0) as total_remove_from_cart_qty,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.unique_session_count, 0) as order_session_count,
    coalesce(o.total_shipping_revenue, 0) as total_shipping_revenue,
    coalesce(r.returns_count, 0) as returns_count,
    coalesce(r.refunded_orders_count, 0) as refunded_orders_count,
    case
        when coalesce(s.sessions_count, 0) = 0 then null
        else coalesce(o.total_orders, 0)::float / s.sessions_count
    end as session_to_order_rate
from sessions s
full outer join engagement e
    on s.date_day = e.date_day
full outer join orders o
    on coalesce(s.date_day, e.date_day) = o.date_day
full outer join returns_data r
    on coalesce(s.date_day, e.date_day, o.date_day) = r.date_day
order by 1