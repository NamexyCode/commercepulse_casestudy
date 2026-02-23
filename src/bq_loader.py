import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET")


def get_bq_client():
    return bigquery.Client(project=PROJECT)

def table_ref(table_name: str) -> str:
    return f"{PROJECT}.{DATASET}.{table_name}"


# ─── Core Loader ─────────────────────────────────────────────────────────────

def load_dataframe(client, df: pd.DataFrame, table_name: str, write_mode: str = "WRITE_APPEND"):
    if df.empty:
        print(f"  ⚠️  No data to load into {table_name}")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref(table_name), job_config=job_config)
    job.result()
    print(f"  ✅ Loaded {len(df)} rows into {table_name}")


# ─── Orders (upsert via MERGE) ────────────────────────────────────────────────

def upsert_orders(client, df: pd.DataFrame):
    if df.empty:
        print("  ⚠️  No orders to upsert")
        return

    staging_table = "fact_orders_staging"

    # Step 1: Load to staging
    load_dataframe(client, df, staging_table, write_mode="WRITE_TRUNCATE")

    # Step 2: Create fact_orders if it doesn't exist
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_ref('fact_orders')}` (
        order_id STRING,
        customer_id STRING,
        vendor_id STRING,
        status STRING,
        gross_amount FLOAT64,
        currency STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        _ingested_at TIMESTAMP
    )
    """
    client.query(create_sql).result()

    # Step 3: MERGE staging into main table
    merge_sql = f"""
    MERGE `{table_ref('fact_orders')}` AS target
    USING `{table_ref(staging_table)}` AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN
        UPDATE SET
            status = source.status,
            gross_amount = source.gross_amount,
            updated_at = source.updated_at,
            _ingested_at = source._ingested_at
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_id, vendor_id, status, gross_amount,
                currency, created_at, updated_at, _ingested_at)
        VALUES (source.order_id, source.customer_id, source.vendor_id,
                source.status, source.gross_amount, source.currency,
                source.created_at, source.updated_at, source._ingested_at)
    """
    client.query(merge_sql).result()
    print(f"  ✅ Upserted {len(df)} orders into fact_orders")


# ─── Payments (append, deduplicated) ─────────────────────────────────────────

def load_payments(client, df: pd.DataFrame):
    if df.empty:
        print("  ⚠️  No payments to load")
        return

    try:
        existing = client.query(
            f"SELECT payment_id FROM `{table_ref('fact_payments')}`"
        ).to_dataframe()
        existing_ids = set(existing["payment_id"].tolist())
        df = df[~df["payment_id"].isin(existing_ids)]
        print(f"  🔍 {len(df)} new payments after deduplication")
    except Exception:
        print("  ℹ️  fact_payments doesn't exist yet, loading all rows")

    load_dataframe(client, df, "fact_payments")


# ─── Refunds (append, deduplicated) ──────────────────────────────────────────

def load_refunds(client, df: pd.DataFrame):
    if df.empty:
        print("  ⚠️  No refunds to load")
        return

    try:
        existing = client.query(
            f"SELECT refund_id FROM `{table_ref('fact_refunds')}`"
        ).to_dataframe()
        existing_ids = set(existing["refund_id"].tolist())
        df = df[~df["refund_id"].isin(existing_ids)]
        print(f"  🔍 {len(df)} new refunds after deduplication")
    except Exception:
        print("  ℹ️  fact_refunds doesn't exist yet, loading all rows")

    load_dataframe(client, df, "fact_refunds")


# ─── Shipments (append, deduplicated) ────────────────────────────────────────

def load_shipments(client, df: pd.DataFrame):
    if df.empty:
        print("  ⚠️  No shipments to load")
        return

    try:
        existing = client.query(
            f"SELECT shipment_id FROM `{table_ref('fact_shipments')}`"
        ).to_dataframe()
        existing_ids = set(existing["shipment_id"].tolist())
        df = df[~df["shipment_id"].isin(existing_ids)]
        print(f"  🔍 {len(df)} new shipments after deduplication")
    except Exception:
        print("  ℹ️  fact_shipments doesn't exist yet, loading all rows")

    load_dataframe(client, df, "fact_shipments")


# ─── Daily Summary ────────────────────────────────────────────────────────────

def compute_and_load_daily_summary(client):
    # Create fact_order_daily if it doesn't exist
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_ref('fact_order_daily')}` (
        report_date DATE,
        vendor_id STRING,
        total_orders INT64,
        gross_revenue FLOAT64,
        total_refunds FLOAT64,
        net_revenue FLOAT64,
        successful_payments INT64,
        failed_payments INT64,
        _computed_at TIMESTAMP
    )
    """
    client.query(create_sql).result()

    sql = f"""
    INSERT INTO `{table_ref('fact_order_daily')}`
    (report_date, vendor_id, total_orders, gross_revenue,
     total_refunds, net_revenue, successful_payments, failed_payments, _computed_at)

    WITH daily_orders AS (
        SELECT
            DATE(created_at) AS report_date,
            vendor_id,
            COUNT(*) AS total_orders,
            SUM(gross_amount) AS gross_revenue
        FROM `{table_ref('fact_orders')}`
        WHERE DATE(created_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1, 2
    ),
    daily_refunds AS (
        SELECT
            DATE(refunded_at) AS report_date,
            vendor_id,
            SUM(amount) AS total_refunds
        FROM `{table_ref('fact_refunds')}`
        WHERE DATE(refunded_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1, 2
    ),
    daily_payments AS (
        SELECT
            DATE(attempted_at) AS report_date,
            vendor_id,
            COUNTIF(status = 'success') AS successful_payments,
            COUNTIF(status = 'failed') AS failed_payments
        FROM `{table_ref('fact_payments')}`
        WHERE DATE(attempted_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1, 2
    )
    SELECT
        o.report_date,
        o.vendor_id,
        o.total_orders,
        o.gross_revenue,
        COALESCE(r.total_refunds, 0) AS total_refunds,
        o.gross_revenue - COALESCE(r.total_refunds, 0) AS net_revenue,
        COALESCE(p.successful_payments, 0) AS successful_payments,
        COALESCE(p.failed_payments, 0) AS failed_payments,
        CURRENT_TIMESTAMP() AS _computed_at
    FROM daily_orders o
    LEFT JOIN daily_refunds r USING (report_date, vendor_id)
    LEFT JOIN daily_payments p USING (report_date, vendor_id)
    """

    client.query(sql).result()
    print("  ✅ Daily summary computed and loaded")