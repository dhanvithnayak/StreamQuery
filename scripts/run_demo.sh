#!/bin/bash
set -e

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}==============================================================${NC}"
echo -e "${CYAN}    StreamQuery: Streaming ELT Pipeline & NL Query Agent      ${NC}"
echo -e "${CYAN}==============================================================${NC}"

# 1. Check containers
echo -e "\n${YELLOW}[1/5] Checking Docker Compose Services...${NC}"
docker compose ps

# 2. Produce synthetic events inside container (or host)
echo -e "\n${YELLOW}[2/5] Emitting 50 Synthetic Order Events via Kafka Producer...${NC}"
docker compose exec airflow-webserver python /opt/airflow/producer/produce_events.py --broker kafka:9092 --count 50 --interval 0.02

# 3. Micro-batch ingest into PostgreSQL raw warehouse
echo -e "\n${YELLOW}[3/5] Ingesting Kafka Events into PostgreSQL Warehouse (raw.orders)...${NC}"
docker compose exec airflow-webserver python /opt/airflow/consumer/load_to_warehouse.py --broker kafka:9092 --db-host postgres --batch-mode --poll-timeout 3.0

# 4. Run dbt transformations and automated tests
echo -e "\n${YELLOW}[4/5] Running dbt Staging -> Marts Transformations & Contract Tests...${NC}"
docker compose exec airflow-webserver dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
docker compose exec airflow-webserver dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

# 5. Query the NL Agent API
echo -e "\n${YELLOW}[5/5] Querying Natural Language Text-to-SQL Agent...${NC}"

echo -e "\n${GREEN}--- Question 1: Total Sales by Product Category ---${NC}"
curl -s -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What are the total sales by product category?"}' | python3 -m json.tool

echo -e "\n${GREEN}--- Question 2: Top 5 Customers by Total Spend ---${NC}"
curl -s -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Who are our top 5 customers by total spend?"}' | python3 -m json.tool

echo -e "\n${GREEN}--- Question 3: Regional Sales & Order Volume ---${NC}"
curl -s -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me sales and order counts breakdown by region"}' | python3 -m json.tool

echo -e "\n${CYAN}==============================================================${NC}"
echo -e "${GREEN}    StreamQuery End-to-End Walkthrough Completed Successfully!${NC}"
echo -e "${CYAN}==============================================================${NC}"
