with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_summary as (
    select
        customer_id,
        max(customer_name) as customer_name,
        max(customer_email) as customer_email,
        min(ordered_at) as first_order_date,
        max(ordered_at) as last_order_date,
        count(distinct order_id) as total_orders,
        count(distinct case when order_status = 'completed' then order_id end) as completed_orders,
        coalesce(sum(case when order_status = 'completed' then gross_amount else 0 end), 0) as lifetime_value,
        coalesce(sum(case when order_status = 'completed' then quantity else 0 end), 0) as total_items_purchased
    from orders
    group by customer_id
),

final as (
    select
        customer_id,
        customer_name,
        customer_email,
        first_order_date,
        last_order_date,
        total_orders,
        completed_orders,
        lifetime_value,
        total_items_purchased,
        case
            when lifetime_value >= 1000 then 'VIP'
            when lifetime_value >= 400 then 'High Value'
            when total_orders > 1 then 'Repeat'
            else 'Standard'
        end as customer_tier
    from customer_summary
)

select * from final
