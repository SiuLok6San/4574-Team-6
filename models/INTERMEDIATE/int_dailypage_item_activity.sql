-- Combines page views and item views into daily engagement metrics.
with page_views as (

    select
        cast(view_at as date) as date_day,
        count(*) as page_views_count,
        count(distinct session_id) as page_view_sessions_count
    from {{ ref('WEB_PAGE_VIEWS') }}
    where view_at is not null
    group by 1

),

item_views as (

    select
        cast(s.session_at as date) as date_day,
        count(*) as item_views_count,
        count(distinct i.session_id) as item_view_sessions_count,
        sum(coalesce(i.add_to_cart_quantity, 0)) as total_add_to_cart_qty,
        sum(coalesce(i.remove_from_cart_quantity, 0)) as total_remove_from_cart_qty
    from {{ ref('WEB_ITEM_VIEWS') }} i
    left join {{ ref('int_sessions') }} s
        on i.session_id = s.session_id
    where s.session_at is not null
    group by 1

)

select
    coalesce(p.date_day, i.date_day) as date_day,
    coalesce(p.page_views_count, 0) as page_views_count,
    coalesce(p.page_view_sessions_count, 0) as page_view_sessions_count,
    coalesce(i.item_views_count, 0) as item_views_count,
    coalesce(i.item_view_sessions_count, 0) as item_view_sessions_count,
    coalesce(i.total_add_to_cart_qty, 0) as total_add_to_cart_qty,
    coalesce(i.total_remove_from_cart_qty, 0) as total_remove_from_cart_qty
from page_views p
full outer join item_views i
    on p.date_day = i.date_day
order by 1