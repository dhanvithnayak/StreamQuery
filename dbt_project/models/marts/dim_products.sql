with orders as (
    select * from {{ ref('stg_orders') }}
),

product_summary as (
    select
        product_id,
        max(product_name) as product_name,
        max(category) as category,
        max(unit_price) as current_unit_price,
        count(distinct order_id) as total_orders_count,
        coalesce(sum(case when order_status = 'completed' then quantity else 0 end), 0) as total_units_sold,
        coalesce(sum(case when order_status = 'completed' then gross_amount else 0 end), 0) as total_revenue,
        round(avg(case when order_status = 'completed' then quantity end)::numeric, 2) as avg_units_per_order
    from orders
    group by product_id
)

select * from product_summary
