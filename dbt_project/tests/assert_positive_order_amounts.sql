-- Custom singular test: Returns any record where gross_amount or quantity is invalid (non-positive)
-- If this query returns ANY rows, dbt test fails.

select
    order_id,
    quantity,
    unit_price,
    gross_amount
from {{ ref('fct_orders') }}
where gross_amount <= 0 or quantity <= 0 or unit_price <= 0
