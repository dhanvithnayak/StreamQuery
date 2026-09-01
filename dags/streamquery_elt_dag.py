"""
Airflow DAG: StreamQuery Streaming ELT Orchestrator
Dependency Chain: Kafka Ingest -> Check Raw Data -> dbt Run -> dbt Test (Schema Contracts) -> dbt Docs
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt_project")


def verify_raw_records(**kwargs):
    """Ensure raw.orders contains records before triggering dbt transformations."""
    host = os.getenv("DBT_HOST", os.getenv("POSTGRES_HOST", "postgres"))
    port = int(os.getenv("DBT_PORT", "5432"))
    dbname = os.getenv("DBT_DATABASE", "warehouse")
    user = os.getenv("DBT_USER", "postgres")
    password = os.getenv("DBT_PASSWORD", "postgres")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.orders;")
        row_count = cur.fetchone()[0]
    conn.close()

    print(f"[Airflow Task] Verified raw.orders contains {row_count} total records.")
    if row_count == 0:
        raise ValueError("raw.orders is empty! Ingestion task found no records to transform.")
    return row_count


with DAG(
    dag_id="streamquery_elt_dag",
    default_args=default_args,
    description="Orchestrates Kafka micro-batch ingest, dbt staging-to-mart transformations, and contract testing",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "kafka", "dbt", "warehouse", "streamquery"],
) as dag:

    # 1. Ingest Kafka events into raw.orders (Micro-batch load)
    ingest_kafka_events = BashOperator(
        task_id="ingest_kafka_events",
        bash_command=(
            "python /opt/airflow/consumer/load_to_warehouse.py "
            "--batch-mode "
            "--poll-timeout 3.0 "
            "--batch-size 200"
        ),
        env={
            "KAFKA_BROKER": os.getenv("KAFKA_BROKER", "kafka:9092"),
            "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "orders.events"),
            "DBT_HOST": os.getenv("DBT_HOST", "postgres"),
            "DBT_PORT": os.getenv("DBT_PORT", "5432"),
            "DBT_DATABASE": os.getenv("DBT_DATABASE", "warehouse"),
            "DBT_USER": os.getenv("DBT_USER", "postgres"),
            "DBT_PASSWORD": os.getenv("DBT_PASSWORD", "postgres"),
        },
    )

    # 2. Sensor check to ensure data is present
    verify_raw_data = PythonOperator(
        task_id="verify_raw_data",
        python_callable=verify_raw_records,
    )

    # 3. dbt run: Transform staging -> intermediate -> marts
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "DBT_TARGET": os.getenv("DBT_TARGET", "postgres"),
            "DBT_HOST": os.getenv("DBT_HOST", "postgres"),
            "DBT_PORT": os.getenv("DBT_PORT", "5432"),
            "DBT_DATABASE": os.getenv("DBT_DATABASE", "warehouse"),
            "DBT_USER": os.getenv("DBT_USER", "postgres"),
            "DBT_PASSWORD": os.getenv("DBT_PASSWORD", "postgres"),
        },
    )

    # 4. dbt test: Automated data-quality & schema contract tests (FAILS DAG IF TEST TRIPS)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "DBT_TARGET": os.getenv("DBT_TARGET", "postgres"),
            "DBT_HOST": os.getenv("DBT_HOST", "postgres"),
            "DBT_PORT": os.getenv("DBT_PORT", "5432"),
            "DBT_DATABASE": os.getenv("DBT_DATABASE", "warehouse"),
            "DBT_USER": os.getenv("DBT_USER", "postgres"),
            "DBT_PASSWORD": os.getenv("DBT_PASSWORD", "postgres"),
        },
    )

    # 5. dbt docs generate: Generate fresh catalog documentation
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "DBT_TARGET": os.getenv("DBT_TARGET", "postgres"),
            "DBT_HOST": os.getenv("DBT_HOST", "postgres"),
            "DBT_PORT": os.getenv("DBT_PORT", "5432"),
            "DBT_DATABASE": os.getenv("DBT_DATABASE", "warehouse"),
            "DBT_USER": os.getenv("DBT_USER", "postgres"),
            "DBT_PASSWORD": os.getenv("DBT_PASSWORD", "postgres"),
        },
    )

    # Define DAG execution dependencies
    ingest_kafka_events >> verify_raw_data >> dbt_run >> dbt_test >> dbt_docs
