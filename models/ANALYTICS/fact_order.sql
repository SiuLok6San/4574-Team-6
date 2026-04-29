with order_returns as (

    select
        order_id,
        session_id,
        order_at,
        client_name,
        payment_method,
        shipping_address,
        shipping_cost,
        phone,
        state,
        tax_rate,
        first_returned_at,
        refunded_flag,
        return_record_count
    from {{ ref('int_order_returns') }}

),

session_item_revenue as (

    select
        session_id,
        sum(
            coalesce(add_to_cart_quantity, 0)
            * coalesce(price_per_unit, 0)
        ) as item_revenue
    from {{ ref('WEB_ITEM_VIEWS') }}
    group by 1

),

final as (

    select
        o.order_id,
        o.session_id,
        o.order_at,
        o.client_name,
        o.payment_method,
        o.shipping_address,
        o.shipping_cost,
        o.phone,
        o.state,
        o.tax_rate,
        o.first_returned_at,
        o.refunded_flag,
        o.return_record_count,

        coalesce(r.item_revenue, 0) as item_revenue,

        coalesce(r.item_revenue, 0)
            + coalesce(o.shipping_cost, 0) as order_revenue,

        (
            coalesce(r.item_revenue, 0)
            + coalesce(o.shipping_cost, 0)
        ) * (1 + coalesce(o.tax_rate, 0)) as total_revenue

    from order_returns o
    left join session_item_revenue r
        on o.session_id = r.session_id

)

select * from final