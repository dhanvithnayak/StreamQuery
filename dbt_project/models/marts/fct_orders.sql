with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    gross_amount,
    order_status,
    case when order_status = 'completed' then 1 else 0 end as is_completed,
    region,
    ordered_at,
    order_date,
    extract(year from ordered_at)::integer as order_year,
    extract(month from ordered_at)::integer as order_month,
    to_char(ordered_at, 'Dy') as order_day_of_week
from orders
