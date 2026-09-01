#!/usr/bin/env python3
"""
Consumer: Ingests Kafka order events into PostgreSQL Raw Warehouse Table.
Supports continuous daemon mode or micro-batch execution (ideal for Airflow task triggers).
"""

import os
import sys
import json
import time
import argparse
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor

try:
    from confluent_kafka import Consumer as ConfluentConsumer, KafkaError
    HAS_CONFLUENT = True
except ImportError:
    HAS_CONFLUENT = False

try:
    from kafka import KafkaConsumer as PyKafkaConsumer
    HAS_PYKAFKA = True
except ImportError:
    HAS_PYKAFKA = False


DDL_RAW_ORDERS = """
CREATE SCHEMA IF NOT EXISTS raw;

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
"""

UPSERT_SQL = """
INSERT INTO raw.orders (
    order_id,
    customer_id,
    customer_name,
    customer_email,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    order_status,
    region,
    ordered_at,
    raw_payload
) VALUES (
    %(order_id)s,
    %(customer_id)s,
    %(customer_name)s,
    %(customer_email)s,
    %(product_id)s,
    %(product_name)s,
    %(category)s,
    %(quantity)s,
    %(unit_price)s,
    %(order_status)s,
    %(region)s,
    %(ordered_at)s,
    %(raw_payload)s
)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    customer_name = EXCLUDED.customer_name,
    customer_email = EXCLUDED.customer_email,
    product_id = EXCLUDED.product_id,
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    order_status = EXCLUDED.order_status,
    region = EXCLUDED.region,
    ordered_at = EXCLUDED.ordered_at,
    raw_payload = EXCLUDED.raw_payload,
    ingested_at = CURRENT_TIMESTAMP;
"""


def get_db_connection(host, port, dbname, user, password, retries=10, delay=2):
    """Establish connection to PostgreSQL warehouse with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=5
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            if attempt == retries:
                raise e
            print(f"[RETRY {attempt}/{retries}] Waiting for Postgres ({dbname} at {host}:{port})... Error: {e}")
            time.sleep(delay)


def init_warehouse_schema(conn):
    """Ensure raw schema and orders table exist."""
    with conn.cursor() as cur:
        cur.execute(DDL_RAW_ORDERS)
    conn.commit()
    print("[INFO] Warehouse table 'raw.orders' schema validated.")


class UniversalKafkaConsumer:
    """Universal Kafka consumer supporting confluent-kafka or kafka-python."""
    def __init__(self, broker: str, topic: str, group_id: str = "praxis-warehouse-loader"):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        self.client_type = None
        self.consumer = None

        if HAS_CONFLUENT:
            try:
                self.consumer = ConfluentConsumer({
                    "bootstrap.servers": broker,
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False
                })
                self.consumer.subscribe([topic])
                self.client_type = "confluent"
                print(f"[INFO] Using Confluent Kafka Consumer connected to {broker}, topic '{topic}'")
            except Exception as e:
                print(f"[WARN] Confluent consumer init failed: {e}")

        if not self.consumer and HAS_PYKAFKA:
            try:
                self.consumer = PyKafkaConsumer(
                    topic,
                    bootstrap_servers=broker.split(","),
                    group_id=group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=3000
                )
                self.client_type = "pykafka"
                print(f"[INFO] Using kafka-python Consumer connected to {broker}, topic '{topic}'")
            except Exception as e:
                print(f"[WARN] kafka-python consumer init failed: {e}")

        if not self.consumer:
            raise RuntimeError(f"Could not initialize any Kafka consumer for broker {broker}")

    def poll_batch(self, max_records=50, timeout_sec=2.0) -> list:
        records = []
        start = time.time()

        if self.client_type == "confluent":
            while len(records) < max_records and (time.time() - start) < timeout_sec:
                msg = self.consumer.poll(timeout=0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"[ERROR] Consumer error: {msg.error()}")
                        break
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    records.append(payload)
                except Exception as e:
                    print(f"[ERROR] Failed to parse message: {e}")
        elif self.client_type == "pykafka":
            raw_batches = self.consumer.poll(timeout_ms=int(timeout_sec * 1000), max_records=max_records)
            for tp, messages in raw_batches.items():
                for msg in messages:
                    records.append(msg.value)

        return records

    def commit(self):
        if self.client_type == "confluent":
            self.consumer.commit(asynchronous=False)
        elif self.client_type == "pykafka":
            self.consumer.commit()

    def close(self):
        if self.consumer:
            self.consumer.close()


def insert_records(conn, records: list) -> int:
    """Insert or update batch of records into PostgreSQL raw.orders."""
    if not records:
        return 0

    prepared_data = []
    for r in records:
        prepared_data.append({
            "order_id": r.get("order_id"),
            "customer_id": r.get("customer_id"),
            "customer_name": r.get("customer_name"),
            "customer_email": r.get("customer_email"),
            "product_id": r.get("product_id"),
            "product_name": r.get("product_name"),
            "category": r.get("category"),
            "quantity": int(r.get("quantity", 1)),
            "unit_price": float(r.get("unit_price", 0.0)),
            "order_status": r.get("order_status", "pending"),
            "region": r.get("region", "NA"),
            "ordered_at": r.get("ordered_at"),
            "raw_payload": json.dumps(r)
        })

    with conn.cursor() as cur:
        execute_batch(cur, UPSERT_SQL, prepared_data, page_size=100)
    conn.commit()
    return len(prepared_data)


def main():
    parser = argparse.ArgumentParser(description="Praxis-Data Kafka to Warehouse Consumer")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:29092"), help="Kafka broker")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders.events"), help="Kafka topic")
    parser.add_argument("--group-id", default="praxis-warehouse-loader", help="Kafka consumer group ID")
    parser.add_argument("--db-host", default=os.getenv("DBT_HOST", os.getenv("POSTGRES_HOST", "localhost")), help="Postgres host")
    parser.add_argument("--db-port", default=int(os.getenv("DBT_PORT", os.getenv("POSTGRES_PORT", "5432"))), help="Postgres port")
    parser.add_argument("--db-name", default=os.getenv("DBT_DATABASE", os.getenv("WAREHOUSE_DB", "warehouse")), help="Postgres db name")
    parser.add_argument("--db-user", default=os.getenv("DBT_USER", os.getenv("POSTGRES_USER", "postgres")), help="Postgres user")
    parser.add_argument("--db-password", default=os.getenv("DBT_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")), help="Postgres password")
    parser.add_argument("--batch-mode", action="store_true", help="Run a single micro-batch and exit (ideal for Airflow)")
    parser.add_argument("--batch-size", type=int, default=100, help="Max records per batch")
    parser.add_argument("--poll-timeout", type=float, default=3.0, help="Poll timeout in seconds")
    parser.add_argument("--daemon", action="store_true", help="Run continuously")
    args = parser.parse_args()

    print("==================================================")
    print("  Praxis-Data Warehouse Loader Starting")
    print(f"  Kafka:     {args.broker} (Topic: {args.topic})")
    print(f"  Warehouse: {args.db_user}@{args.db_host}:{args.db_port}/{args.db_name}")
    print(f"  Mode:      {'Daemon (Continuous)' if args.daemon else 'Batch Ingestion'}")
    print("==================================================")

    # 1. Connect to PostgreSQL Warehouse
    conn = get_db_connection(args.db_host, args.db_port, args.db_name, args.db_user, args.db_password)
    init_warehouse_schema(conn)

    # 2. Connect to Kafka
    try:
        consumer = UniversalKafkaConsumer(args.broker, args.topic, args.group_id)
    except Exception as e:
        print(f"[FATAL] Could not connect to Kafka: {e}", file=sys.stderr)
        conn.close()
        sys.exit(1)

    total_ingested = 0
    empty_polls = 0
    max_empty_polls = 2 if args.batch_mode else 999999

    try:
        while True:
            records = consumer.poll_batch(max_records=args.batch_size, timeout_sec=args.poll_timeout)
            if records:
                empty_polls = 0
                count = insert_records(conn, records)
                consumer.commit()
                total_ingested += count
                print(f"[INGEST] Processed and committed {count} orders to raw.orders (Total this session: {total_ingested})")
            else:
                empty_polls += 1
                if args.batch_mode:
                    print(f"[BATCH] No new messages found after {args.poll_timeout}s timeout.")
                    if empty_polls >= max_empty_polls:
                        break
                else:
                    time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[INFO] Consumer stopped by user.")
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw.orders;")
            total_in_db = cur.fetchone()[0]
        conn.close()
        consumer.close()
        print("==================================================")
        print(f"  Loader Finished. Total Ingested This Run: {total_ingested}")
        print(f"  Current Total Rows in raw.orders:        {total_in_db}")
        print("==================================================")


if __name__ == "__main__":
    main()
