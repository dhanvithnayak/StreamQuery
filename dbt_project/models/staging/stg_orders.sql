with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        trim(customer_name) as customer_name,
        lower(trim(customer_email)) as customer_email,
        product_id,
        trim(product_name) as product_name,
        trim(category) as category,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(10, 2)) as unit_price,
        cast(quantity * unit_price as numeric(10, 2)) as gross_amount,
        lower(trim(order_status)) as order_status,
        upper(trim(region)) as region,
        cast(ordered_at as timestamp with time zone) as ordered_at,
        cast(ordered_at as date) as order_date,
        ingested_at
    from source
)

select * from renamed
