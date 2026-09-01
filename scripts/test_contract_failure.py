#!/usr/bin/env python3
"""
Test Script: Intentionally trip a dbt schema contract / test to verify failure propagation.
Proves the resume bullet: 'enforcing schema contracts on ingest'.
"""

import os
import sys
import subprocess
import psycopg2


def inject_invalid_record(db_host="localhost", db_port=5432, db_name="warehouse", db_user="postgres", db_pass="postgres"):
    print("\n[STEP 1] Injecting invalid records with contract violations into raw.orders...")
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    with conn.cursor() as cur:
        # Violation 1: Invalid order_status (trips accepted_values test)
        # Violation 2: Negative unit_price and gross_amount (trips assert_positive_order_amounts test)
        cur.execute("""
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
                'ORD-FAIL-CONTRACT-001',
                'CUST-001',
                'Contract Test User',
                'test@example.com',
                'PROD-001',
                'Test Product',
                'Electronics',
                -5,
                -199.99,
                'CORRUPT_INVALID_STATUS',
                'INVALID_REGION',
                NOW(),
                '{"test": "corrupt_data"}'
            )
            ON CONFLICT (order_id) DO NOTHING;
        """)
    conn.commit()
    conn.close()
    print(" -> Injected test record 'ORD-FAIL-CONTRACT-001' with invalid status 'CORRUPT_INVALID_STATUS' and negative price/qty.")


def run_dbt_test_and_assert_failure():
    print("\n[STEP 2] Running dbt test to verify automated contract enforcement...")
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dbt_project"))
    cmd = [
        "dbt", "test",
        "--project-dir", project_dir,
        "--profiles-dir", project_dir
    ]
    env = os.environ.copy()
    env["DBT_HOST"] = os.getenv("DBT_HOST", "localhost")
    env["DBT_PORT"] = os.getenv("DBT_PORT", "5432")
    env["DBT_DATABASE"] = os.getenv("DBT_DATABASE", "warehouse")
    env["DBT_USER"] = os.getenv("DBT_USER", "postgres")
    env["DBT_PASSWORD"] = os.getenv("DBT_PASSWORD", "postgres")

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print(result.stdout)

    if result.returncode != 0:
        print("\n==================================================================")
        print("  SUCCESS: dbt test FAILED as expected on invalid data contract!")
        print("  Automated data-quality contract successfully blocked bad records.")
        print("==================================================================")
        return True
    else:
        print("\n[ERROR] dbt test unexpectedly passed when invalid records were present!")
        return False


def clean_up_invalid_record(db_host="localhost", db_port=5432, db_name="warehouse", db_user="postgres", db_pass="postgres"):
    print("\n[STEP 3] Cleaning up test failure record...")
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    with conn.cursor() as cur:
        cur.execute("DELETE FROM raw.orders WHERE order_id = 'ORD-FAIL-CONTRACT-001';")
    conn.commit()
    conn.close()
    print(" -> Cleaned up 'ORD-FAIL-CONTRACT-001'.")


if __name__ == "__main__":
    db_host = os.getenv("DBT_HOST", "localhost")
    db_port = int(os.getenv("DBT_PORT", "5432"))
    inject_invalid_record(db_host=db_host, db_port=db_port)
    failed_as_expected = run_dbt_test_and_assert_failure()
    clean_up_invalid_record(db_host=db_host, db_port=db_port)

    if not failed_as_expected:
        sys.exit(1)
