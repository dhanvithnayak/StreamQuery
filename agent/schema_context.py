"""
Schema Context Provider: Formats data mart catalogs, column types, descriptions,
and join relationships into structured context for the Text-to-SQL LLM Agent.
"""

MART_SCHEMA_DDL = """
-- Schema: analytics (Data Marts Layer)

-- Table 1: analytics.dim_customers
-- Granularity: 1 row per unique customer
CREATE TABLE analytics.dim_customers (
    customer_id VARCHAR(64) PRIMARY KEY,       -- Unique customer ID (e.g. CUST-001)
    customer_name VARCHAR(255) NOT NULL,        -- Full name of customer
    customer_email VARCHAR(255),                -- Email address
    first_order_date TIMESTAMP WITH TIME ZONE,  -- Timestamp of customer's first order
    last_order_date TIMESTAMP WITH TIME ZONE,   -- Timestamp of customer's most recent order
    total_orders INTEGER,                       -- Lifetime total orders placed across all statuses
    completed_orders INTEGER,                   -- Lifetime total completed orders
    lifetime_value NUMERIC(12, 2),              -- Total spend from completed orders in USD
    total_items_purchased INTEGER,              -- Total item units purchased across completed orders
    customer_tier VARCHAR(50)                   -- Value tier: 'VIP' (>= $1000), 'High Value' (>= $400), 'Repeat', 'Standard'
);

-- Table 2: analytics.dim_products
-- Granularity: 1 row per unique product SKU
CREATE TABLE analytics.dim_products (
    product_id VARCHAR(64) PRIMARY KEY,         -- Unique product SKU (e.g. PROD-001)
    product_name VARCHAR(255) NOT NULL,         -- Descriptive product title
    category VARCHAR(100) NOT NULL,             -- Category: 'Electronics', 'Apparel', 'Home & Kitchen', 'Books'
    current_unit_price NUMERIC(10, 2),          -- Current listing unit price
    total_orders_count INTEGER,                 -- Number of distinct orders containing this item
    total_units_sold INTEGER,                   -- Lifetime units fulfilled in completed orders
    total_revenue NUMERIC(12, 2),               -- Lifetime revenue generated from completed orders
    avg_units_per_order NUMERIC(6, 2)           -- Average quantity per order
);

-- Table 3: analytics.fct_orders
-- Granularity: 1 row per order transaction
CREATE TABLE analytics.fct_orders (
    order_id VARCHAR(64) PRIMARY KEY,           -- Unique order code (e.g. ORD-202608-...)
    customer_id VARCHAR(64) NOT NULL,           -- Foreign key -> dim_customers.customer_id
    product_id VARCHAR(64) NOT NULL,            -- Foreign key -> dim_products.product_id
    quantity INTEGER NOT NULL,                  -- Units ordered in this transaction
    unit_price NUMERIC(10, 2) NOT NULL,         -- Price per unit at purchase time
    gross_amount NUMERIC(10, 2) NOT NULL,       -- Total order line amount (quantity * unit_price)
    order_status VARCHAR(50) NOT NULL,          -- Status: 'completed', 'pending', 'cancelled', 'returned'
    is_completed INTEGER NOT NULL,              -- 1 if completed, 0 if pending/cancelled/returned
    region VARCHAR(50) NOT NULL,                -- Sales territory: 'NA', 'EU', 'APAC', 'LATAM'
    ordered_at TIMESTAMP WITH TIME ZONE,        -- Exact timestamp when order was placed
    order_date DATE,                            -- Date portion of order timestamp
    order_year INTEGER,                         -- Year of order (e.g. 2026)
    order_month INTEGER,                        -- Month number (1 - 12)
    order_day_of_week VARCHAR(10)               -- Day of week abbreviation ('Mon', 'Tue', 'Wed', etc.)
);
"""

BUSINESS_RULES = """
Key Business Rules & Query Guidelines:
1. All queries MUST target tables in the 'analytics' schema (`analytics.fct_orders`, `analytics.dim_customers`, `analytics.dim_products`).
2. When calculating 'revenue', 'sales', or 'total spend', filter by `order_status = 'completed'` or `is_completed = 1`, or query pre-aggregated columns in `dim_customers.lifetime_value` or `dim_products.total_revenue`.
3. To join orders with customers: `JOIN analytics.dim_customers c ON f.customer_id = c.customer_id`
4. To join orders with products: `JOIN analytics.dim_products p ON f.product_id = p.product_id`
5. Always use explicit PostgreSQL SQL syntax.
6. Return only read-only `SELECT` statements. Never output `INSERT`, `UPDATE`, `DELETE`, `DROP`, or `ALTER`.
7. Limit large result sets with `LIMIT 50` unless an explicit limit is requested.
"""


def get_schema_context_prompt() -> str:
    """Returns the complete formatted schema context for prompt injection."""
    return f"""### WAREHOUSE DATA MARTS SCHEMA (PostgreSQL):
{MART_SCHEMA_DDL}

### BUSINESS DEFINITIONS & GUIDELINES:
{BUSINESS_RULES}
"""
