#!/usr/bin/env python3
"""
Producer: Synthetic Order Event Generator using Faker
Emits realistic e-commerce order events into a Kafka topic.
"""

import os
import sys
import json
import time
import uuid
import random
import argparse
from datetime import datetime, timezone, timedelta
from faker import Faker

try:
    from confluent_kafka import Producer as ConfluentProducer
    HAS_CONFLUENT = True
except ImportError:
    HAS_CONFLUENT = False

try:
    from kafka import KafkaProducer as PyKafkaProducer
    HAS_PYKAFKA = True
except ImportError:
    HAS_PYKAFKA = False

# Seed faker for deterministic customer profiles with realistic variety
fake = Faker()
Faker.seed(42)
random.seed(42)

# Fixed Product Catalog for realistic dimension tables
PRODUCT_CATALOG = [
    {"product_id": "PROD-001", "product_name": "Ultra Wireless Noise-Cancelling Headphones", "category": "Electronics", "unit_price": 249.99},
    {"product_id": "PROD-002", "product_name": "Ergonomic Mechanical Keyboard RGB", "category": "Electronics", "unit_price": 129.50},
    {"product_id": "PROD-003", "product_name": "4K Ultra-HD 27-inch IPS Monitor", "category": "Electronics", "unit_price": 389.00},
    {"product_id": "PROD-004", "product_name": "Smart Fitness Tracker & Heart Rate Monitor", "category": "Electronics", "unit_price": 79.99},
    {"product_id": "PROD-005", "product_name": "Organic Cotton Crewneck T-Shirt", "category": "Apparel", "unit_price": 28.00},
    {"product_id": "PROD-006", "product_name": "Waterproof All-Weather Hiking Jacket", "category": "Apparel", "unit_price": 145.00},
    {"product_id": "PROD-007", "product_name": "Comfort Stretch Denim Jeans", "category": "Apparel", "unit_price": 64.50},
    {"product_id": "PROD-008", "product_name": "Merino Wool Thermal Socks (3-pack)", "category": "Apparel", "unit_price": 22.00},
    {"product_id": "PROD-009", "product_name": "Stainless Steel Espresso Machine", "category": "Home & Kitchen", "unit_price": 299.95},
    {"product_id": "PROD-010", "product_name": "Cast Iron Dutch Oven 6-Quart", "category": "Home & Kitchen", "unit_price": 89.90},
    {"product_id": "PROD-011", "product_name": "Chef Knife 8-Inch High-Carbon Steel", "category": "Home & Kitchen", "unit_price": 55.00},
    {"product_id": "PROD-012", "product_name": "Smart Temperature-Control Ceramic Mug", "category": "Home & Kitchen", "unit_price": 99.00},
    {"product_id": "PROD-013", "product_name": "Designing Data-Intensive Applications", "category": "Books", "unit_price": 42.50},
    {"product_id": "PROD-014", "product_name": "Fundamentals of Data Engineering", "category": "Books", "unit_price": 48.00},
    {"product_id": "PROD-015", "product_name": "The Pragmatic Programmer (20th Anniversary)", "category": "Books", "unit_price": 39.95}
]

# Pre-generate 40 persistent customer profiles for repeat buyer dynamics
CUSTOMERS = [
    {
        "customer_id": f"CUST-{str(i+1).zfill(3)}",
        "customer_name": fake.name(),
        "customer_email": fake.email()
    }
    for i in range(40)
]

REGIONS = ["NA", "EU", "APAC", "LATAM"]
REGION_WEIGHTS = [0.45, 0.30, 0.15, 0.10]

STATUSES = ["completed", "pending", "cancelled", "returned"]
STATUS_WEIGHTS = [0.80, 0.10, 0.05, 0.05]


def generate_order_event(order_seq: int = None, backdate_days: int = 0) -> dict:
    """Generate a single realistic order event."""
    customer = random.choice(CUSTOMERS)
    product = random.choice(PRODUCT_CATALOG)
    region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    quantity = random.choices([1, 2, 3, 4, 5, 8], weights=[0.55, 0.25, 0.10, 0.05, 0.03, 0.02], k=1)[0]
    
    order_uuid = uuid.uuid4().hex[:8].upper()
    order_id = f"ORD-{datetime.now().strftime('%Y%m')}-{order_uuid}" if not order_seq else f"ORD-{datetime.now().strftime('%Y%m')}-{str(order_seq).zfill(6)}"
    
    if backdate_days > 0:
        event_time = datetime.now(timezone.utc) - timedelta(
            days=random.uniform(0, backdate_days),
            hours=random.uniform(0, 24),
            minutes=random.uniform(0, 60)
        )
    else:
        event_time = datetime.now(timezone.utc)
    
    unit_price = product["unit_price"]
    
    event = {
        "order_id": order_id,
        "customer_id": customer["customer_id"],
        "customer_name": customer["customer_name"],
        "customer_email": customer["customer_email"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": round(unit_price, 2),
        "order_status": status,
        "region": region,
        "ordered_at": event_time.isoformat()
    }
    return event


class UniversalKafkaProducer:
    """Universal Kafka Producer wrapping confluent-kafka, kafka-python, or dry-run fallback."""
    def __init__(self, bootstrap_servers: str, dry_run: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.dry_run = dry_run
        self.client = None
        self.client_type = "dry_run" if dry_run else None

        if not dry_run:
            if HAS_CONFLUENT:
                try:
                    self.client = ConfluentProducer({
                        "bootstrap.servers": bootstrap_servers,
                        "client.id": "streamquery-order-producer",
                        "acks": "all"
                    })
                    self.client_type = "confluent"
                except Exception as e:
                    print(f"[WARN] Failed to initialize Confluent Kafka producer: {e}", file=sys.stderr)

            if not self.client and HAS_PYKAFKA:
                try:
                    self.client = PyKafkaProducer(
                        bootstrap_servers=bootstrap_servers.split(","),
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        key_serializer=lambda k: k.encode("utf-8") if k else None
                    )
                    self.client_type = "pykafka"
                except Exception as e:
                    print(f"[WARN] Failed to initialize kafka-python producer: {e}", file=sys.stderr)

        if not self.client:
            self.client_type = "dry_run"
            print(f"[INFO] Running in DRY-RUN mode (events will be printed to stdout, not sent to Kafka).")

    def produce(self, topic: str, key: str, value: dict):
        if self.client_type == "confluent":
            payload = json.dumps(value).encode("utf-8")
            self.client.produce(topic, key=key.encode("utf-8"), value=payload)
            self.client.poll(0)
        elif self.client_type == "pykafka":
            self.client.send(topic, key=key, value=value)
        else:
            # Dry run / console output
            pass

    def flush(self):
        if self.client_type == "confluent":
            self.client.flush(timeout=5)
        elif self.client_type == "pykafka":
            self.client.flush()


def main():
    parser = argparse.ArgumentParser(description="StreamQuery Synthetic Order Event Producer")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:29092"), help="Kafka bootstrap broker")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders.events"), help="Kafka destination topic")
    parser.add_argument("--count", type=int, default=50, help="Number of events to produce (0 for infinite if --continuous)")
    parser.add_argument("--interval", type=float, default=0.1, help="Delay between events in seconds (default 0.1s)")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely emitting events")
    parser.add_argument("--backdate-days", type=int, default=7, help="Spread timestamps across the past N days for historical analysis")
    parser.add_argument("--dry-run", action="store_true", help="Print generated events without Kafka broker")
    args = parser.parse_args()

    print(f"==================================================")
    print(f"  StreamQuery Order Event Producer Starting")
    print(f"  Broker:    {args.broker}")
    print(f"  Topic:     {args.topic}")
    print(f"  Count:     {'Continuous' if args.continuous else args.count}")
    print(f"  Interval:  {args.interval}s")
    print(f"==================================================")

    producer = UniversalKafkaProducer(bootstrap_servers=args.broker, dry_run=args.dry_run)

    sent = 0
    start_time = time.time()

    try:
        while True:
            sent += 1
            event = generate_order_event(order_seq=sent, backdate_days=args.backdate_days)
            producer.produce(topic=args.topic, key=event["order_id"], value=event)

            if sent <= 5 or sent % 25 == 0 or (not args.continuous and sent == args.count):
                print(f"[EMIT #{sent:04d}] {event['order_id']} | Cust: {event['customer_id']} | Prod: {event['product_id']} | Qty: {event['quantity']} | Total: ${event['quantity']*event['unit_price']:.2f} | Region: {event['region']} | Status: {event['order_status']}")

            if not args.continuous and sent >= args.count:
                break

            if args.interval > 0:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n[INFO] Producer interrupted by user.")
    finally:
        producer.flush()
        elapsed = time.time() - start_time
        print(f"==================================================")
        print(f"  Producer Finished: Emitted {sent} events in {elapsed:.2f}s ({sent/max(elapsed, 0.001):.1f} events/sec)")
        print(f"==================================================")


if __name__ == "__main__":
    main()
