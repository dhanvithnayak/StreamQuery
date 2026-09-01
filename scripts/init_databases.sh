#!/bin/bash
set -e

echo "Running multi-database initialization script..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
    SELECT 'CREATE DATABASE warehouse' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse')\gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "warehouse" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS analytics;
    
    CREATE TABLE IF NOT EXISTS raw.orders (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(64) UNIQUE NOT NULL,
        customer_id VARCHAR(64) NOT NULL,
        customer_name VARCHAR(255) NOT NULL,
        customer_email VARCHAR(255) NOT NULL,
        product_id VARCHAR(64) NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        category VARCHAR(100) NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(10, 2) NOT NULL,
        order_status VARCHAR(50) NOT NULL,
        region VARCHAR(50) NOT NULL,
        ordered_at TIMESTAMP WITH TIME ZONE NOT NULL,
        raw_payload JSONB,
        ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_orders_ordered_at ON raw.orders(ordered_at);
    CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON raw.orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_product_id ON raw.orders(product_id);
EOSQL

echo "Databases 'airflow' and 'warehouse' (with schemas raw, staging, analytics) initialized successfully."
