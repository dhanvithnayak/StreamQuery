# StreamQuery — Streaming ELT Pipeline with NL Query Agent

[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B?logo=dbt&logoColor=white)](https://getdbt.com)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109+-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org)

An end-to-end, portfolio-ready data engineering platform that ingests real-time e-commerce order streams via **Kafka**, orchestrates batch micro-loading and transformation with **Airflow DAGs**, models dimensional **staging-to-mart layers in dbt** with rigorous data-quality contracts, and exposes an **LLM-powered Text-to-SQL Query Agent** enabling non-technical users to query warehouse marts using natural language.

---

## 🏗️ Architecture Overview

```
[Event Producer] --> [Kafka topic: orders.events] --> [Consumer Micro-Loader] --> [Warehouse: raw.orders]
                                                                                            |
                                                                          [Airflow DAG (streamquery_elt_dag):
                                                                           Ingestion Sensor -> dbt run -> dbt test]
                                                                                            |
                                                                          [dbt: stg_orders -> dim_customers,
                                                                                              dim_products,
                                                                                              fct_orders]
                                                                                            |
                                                                          [NL Query Agent (FastAPI /query):
                                                                           Natural Language -> SQL -> Marts -> JSON]
```

### Stack Components
- **Streaming Ingestion**: Apache Kafka (KRaft mode, single-node broker without Zookeeper overhead) with a `Faker`-based Python order stream generator.
- **Warehouse Storage**: PostgreSQL 15 (default zero-cloud local warehouse) with seamless dbt BigQuery profile compatibility.
- **Orchestration**: Apache Airflow 2.8+ running LocalExecutor, orchestrating ingest sensors, dbt runs, test contract barriers, and catalog documentation generation.
- **Data Transformation & Modeling**: dbt-core with standard dimensional modeling (`staging` -> `marts`: `dim_customers`, `dim_products`, `fct_orders`).
- **Data Quality & Contract Enforcement**: Automated dbt test suite (`unique`, `not_null`, `relationships` referential integrity, `accepted_values`, and custom singular SQL assertions).
- **Natural Language Query Agent**: FastAPI microservice injecting dimensional catalog DDL into LLMs (OpenAI, Anthropic, Gemini, or deterministic dry-run engine) returning executed warehouse results alongside generated SQL.

---

## 🚀 Quickstart (Cold-Start in < 2 Minutes)

### 1. Clone & Start Containers
```bash
# Clone and enter directory
cd StreamQuery

# Copy environment defaults
cp .env.example .env

# Launch all services (Kafka, Postgres, Airflow Webserver/Scheduler, Agent API)
docker compose up -d
```

### 2. Verify Service Health
Visit the web interfaces once containers are running:
- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`)
- **NL Query Agent Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Agent Health Endpoint**: [http://localhost:8000/health](http://localhost:8000/health)

---

## 🎬 2-Minute Live Demo Script

Follow these exact steps to run a live demonstration without improvising:

### Step 1: Emit Stream of Order Events
Generate and stream 50 synthetic order events into Kafka:
```bash
docker compose exec airflow-webserver python /opt/airflow/producer/produce_events.py --broker kafka:9092 --count 50 --interval 0.05
```

### Step 2: Trigger the Airflow ELT Pipeline
Trigger the DAG to ingest from Kafka into `raw.orders`, execute `dbt run`, and enforce `dbt test` quality contracts:
```bash
docker compose exec airflow-webserver airflow dags trigger streamquery_elt_dag
```
You can view real-time task progression in the Airflow UI at [http://localhost:8080](http://localhost:8080).

Alternatively, run the dbt layer directly:
```bash
docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
```

### Step 3: Query Marts via Natural Language Agent
Execute analytical questions against the warehouse data marts:

#### Query A: Sales by Product Category
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What are the total sales by product category?"}' | jq
```
*Generated SQL:*
```sql
SELECT 
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.gross_amount), 2) AS total_revenue
FROM analytics.fct_orders f
JOIN analytics.dim_products p ON f.product_id = p.product_id
WHERE f.is_completed = 1
GROUP BY p.category
ORDER BY total_revenue DESC;
```

#### Query B: Top 5 Customers by Lifetime Spend
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Who are our top 5 customers by total spend?"}' | jq
```

#### Query C: Regional Sales Breakdown
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me sales and order counts breakdown by region"}' | jq
```

---

## 🛡️ Schema Contract Failure Test (Proof of Quality Gates)

To verify that schema contracts actually protect downstream data marts and fail the Airflow DAG when bad data enters:

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/test_contract_failure.py
```
This intentionally injects a corrupt order status and negative unit price into `raw.orders`, runs `dbt test`, proves that dbt fails with exit code `1` and blocks downstream promotion, and cleans up the test record.

---

## 🎯 What This Demonstrates (Resume Bullet Alignment)

| Resume Bullet | Implementation Proof in StreamQuery |
| :--- | :--- |
| **1. "Built a batch/streaming ELT pipeline ingesting order events via Kafka, orchestrated by Airflow DAGs into BigQuery/Postgres"** | - Configured Kafka broker in KRaft mode with Python event publisher.<br>- Implemented micro-batch consumer loading to PostgreSQL `raw.orders` with idempotent upsert keys.<br>- Built Airflow DAG (`streamquery_elt_dag`) linking ingestion sensor -> `dbt run` -> `dbt test` -> `dbt docs`. |
| **2. "Modeled staging-to-mart dbt layers with automated data-quality tests, enforcing schema contracts on ingest"** | - Developed dimensional data models (`stg_orders`, `dim_customers`, `dim_products`, `fct_orders`).<br>- Enforced schema contracts (`unique`, `not_null`, `relationships` foreign keys, `accepted_values` enum checks, and custom singular SQL tests).<br>- Integrated strict failure gates preventing bad data from entering marts. |
| **3. "Extended an LLM gateway into a text-to-SQL agent, letting non-technical users query marts in natural language"** | - Built a FastAPI `/query` endpoint injecting DDL and dimensional schemas into prompt context.<br>- Supports multi-provider adapters (OpenAI, Anthropic, Gemini) with deterministic offline fallback.<br>- Enforced read-only SQL safety guards and executed live queries against PostgreSQL marts. |

---

## 📁 Repository Structure

```
StreamQuery/
├── docker-compose.yml              # Kafka, Postgres, Airflow, and Agent API services
├── Dockerfile.airflow              # Airflow image with dbt-postgres and Kafka dependencies
├── .env.example                    # Sample configuration and credentials
├── requirements.txt                # Python dependencies
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── produce_events.py           # Faker synthetic order stream generator
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── load_to_warehouse.py        # Micro-batch Kafka-to-Warehouse loader
├── dags/
│   └── streamquery_elt_dag.py      # Airflow ELT orchestration DAG
├── dbt_project/
│   ├── dbt_project.yml             # dbt project configurations
│   ├── profiles.yml                # Postgres / BigQuery warehouse targets
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── schema.yml          # Source definitions and staging tests
│   │   │   └── stg_orders.sql
│   │   └── marts/
│   │       ├── schema.yml          # Mart schema, foreign keys & contract tests
│   │       ├── dim_customers.sql   # Customer dimension (LTV, tiers, order counts)
│   │       ├── dim_products.sql    # Product dimension (revenue, units sold)
│   │       └── fct_orders.sql      # Transactional fact table
│   └── tests/
│       └── assert_positive_order_amounts.sql
├── agent/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── schema_context.py           # Mart DDL context and business rules
│   ├── llm_client.py               # Multi-provider Text-to-SQL engine with safety
│   └── main.py                     # FastAPI server with /query, /health, /schema
└── scripts/
    ├── init_databases.sh           # Postgres multi-database & schema initialization
    ├── run_demo.sh                 # 1-click end-to-end demo runner
    └── test_contract_failure.py    # Automated test contract failure verification
```

---

## 📄 License
MIT License. Built for data engineering portfolio demonstrations.
