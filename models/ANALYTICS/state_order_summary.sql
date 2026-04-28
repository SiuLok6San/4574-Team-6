with state_orders as (

    select
        state,

        case
            when state = 'Alabama' then 'US-AL'
            when state = 'Alaska' then 'US-AK'
            when state = 'Arizona' then 'US-AZ'
            when state = 'Arkansas' then 'US-AR'
            when state = 'California' then 'US-CA'
            when state = 'Colorado' then 'US-CO'
            when state = 'Connecticut' then 'US-CT'
            when state = 'Delaware' then 'US-DE'
            when state = 'Florida' then 'US-FL'
            when state = 'Georgia' then 'US-GA'
            when state = 'Hawaii' then 'US-HI'
            when state = 'Idaho' then 'US-ID'
            when state = 'Illinois' then 'US-IL'
            when state = 'Indiana' then 'US-IN'
            when state = 'Iowa' then 'US-IA'
            when state = 'Kansas' then 'US-KS'
            when state = 'Kentucky' then 'US-KY'
            when state = 'Louisiana' then 'US-LA'
            when state = 'Maine' then 'US-ME'
            when state = 'Maryland' then 'US-MD'
            when state = 'Massachusetts' then 'US-MA'
            when state = 'Michigan' then 'US-MI'
            when state = 'Minnesota' then 'US-MN'
            when state = 'Mississippi' then 'US-MS'
            when state = 'Missouri' then 'US-MO'
            when state = 'Montana' then 'US-MT'
            when state = 'Nebraska' then 'US-NE'
            when state = 'Nevada' then 'US-NV'
            when state = 'New Hampshire' then 'US-NH'
            when state = 'New Jersey' then 'US-NJ'
            when state = 'New Mexico' then 'US-NM'
            when state = 'New York' then 'US-NY'
            when state = 'North Carolina' then 'US-NC'
            when state = 'North Dakota' then 'US-ND'
            when state = 'Ohio' then 'US-OH'
            when state = 'Oklahoma' then 'US-OK'
            when state = 'Oregon' then 'US-OR'
            when state = 'Pennsylvania' then 'US-PA'
            when state = 'Rhode Island' then 'US-RI'
            when state = 'South Carolina' then 'US-SC'
            when state = 'South Dakota' then 'US-SD'
            when state = 'Tennessee' then 'US-TN'
            when state = 'Texas' then 'US-TX'
            when state = 'Utah' then 'US-UT'
            when state = 'Vermont' then 'US-VT'
            when state = 'Virginia' then 'US-VA'
            when state = 'Washington' then 'US-WA'
            when state = 'West Virginia' then 'US-WV'
            when state = 'Wisconsin' then 'US-WI'
            when state = 'Wyoming' then 'US-WY'
            else null
        end as state_iso_code,

        count(*) as total_orders,
        count(distinct session_id) as total_sessions_with_orders,
        avg(coalesce(shipping_cost, 0)) as avg_shipping_cost,
        avg(coalesce(tax_rate, 0)) as avg_tax_rate

    from {{ ref('fact_order') }}
    where state is not null
    group by 1, 2
),

final as (

    select
        state,
        state_iso_code,
        total_orders,
        total_sessions_with_orders,
        avg_shipping_cost,
        avg_tax_rate,

        case
            when state in ('New York', 'California') then 5
            when total_orders >= 120 then 4
            when total_orders >= 80 then 3
            when total_orders >= 40 then 2
            else 1
        end as order_volume_tier

    from state_orders
    where state_iso_code is not null
)

select *
from final
order by total_orders desc