with orders as (

    select
        order_id,
        state
    from {{ ref('fact_order') }}

),

returns as (

    select
        order_id
    from {{ ref('fact_return') }}

)

select
    o.state,
    count(distinct o.order_id) as total_orders,
    count(distinct r.order_id) as returned_orders,
    case
        when count(distinct o.order_id) = 0 then null
        else count(distinct r.order_id)::float / count(distinct o.order_id)
    end as return_rate
from orders o
left join returns r
    on o.order_id = r.order_id
where o.state is not null
group by 1
order by return_rate desc