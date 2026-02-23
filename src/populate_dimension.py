# Populate dimension tables and save to warehouse folder
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import os
from pathlib import Path
from pymongo import MongoClient

load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET_ID = os.getenv("BQ_DATASET")
client = bigquery.Client(project=PROJECT_ID)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def table_id(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{name}"

def load_to_bq(df: pd.DataFrame, table_name: str, write_mode: str = "WRITE_TRUNCATE"):
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id(table_name), job_config=job_config)
    job.result()


# ─── dim_date ────────────────────────────────────────────────────────────────

def populate_dim_date() -> pd.DataFrame:
    dates = pd.date_range(start='2023-01-01', end='2026-12-31', freq='D')

    dim_date = pd.DataFrame({
        'date_key': dates.date,  # store as date not timestamp
        'day_of_week': dates.day_name(),
        'week_number': dates.isocalendar().week.astype(int),
        'month': dates.month,
        'month_name': dates.month_name(),
        'quarter': dates.quarter,
        'year': dates.year,
        'is_weekend': dates.dayofweek.isin([5, 6])
    })

    load_to_bq(dim_date, "dim_date")
    print(f"  ✓ dim_date: {len(dim_date)} rows loaded to BigQuery")
    return dim_date


# ─── dim_customer ─────────────────────────────────────────────────────────────

def populate_dim_customer() -> pd.DataFrame:
    """Extract unique customers directly from MongoDB orders_transformed collection."""
    print("  → Extracting customers from MongoDB...")

    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    db = mongo_client[os.getenv("MONGO_DB")]
    collection = db["orders_transformed"]

    docs = list(collection.find(
        {"customer_id": {"$ne": None}},
        {"_id": 0, "customer_id": 1, "created_at": 1}
    ))

    if not docs:
        print("  ⚠ dim_customer: No customer data found in MongoDB")
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)

    customers = df.groupby("customer_id").agg(
        first_order_date=("created_at", "min")
    ).reset_index()

    # Placeholder fields — enrich later when real customer data is available
    customers["customer_name"] = None
    customers["email"] = None
    customers["country"] = None

    load_to_bq(customers, "dim_customer")
    print(f"  ✓ dim_customer: {len(customers)} rows loaded to BigQuery")
    return customers


# ─── dim_vendor ──────────────────────────────────────────────────────────────

def populate_dim_vendor() -> pd.DataFrame:
    """Extract unique vendors from orders_transformed."""
    print("  → Extracting vendors from MongoDB...")

    mongo_client = MongoClient(os.getenv("MONGO_URI"))
    db = mongo_client[os.getenv("MONGO_DB")]
    collection = db["orders_transformed"]

    docs = list(collection.find(
        {"vendor_id": {"$ne": None}},
        {"_id": 0, "vendor_id": 1}
    ))

    if not docs:
        print("  ⚠ dim_vendor: No vendor data found")
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    vendors = df.drop_duplicates(subset=["vendor_id"]).reset_index(drop=True)

    # Placeholder fields
    vendors["vendor_name"] = vendors["vendor_id"]
    vendors["country"] = None
    vendors["category"] = None

    load_to_bq(vendors, "dim_vendor")
    print(f"  ✓ dim_vendor: {len(vendors)} rows loaded to BigQuery")
    return vendors


# ─── dim_product ─────────────────────────────────────────────────────────────

def populate_dim_product() -> pd.DataFrame:
    """Placeholder product dimension — enrich when product data is available."""
    dim_product = pd.DataFrame([{
        'product_id': 'UNKNOWN',
        'product_name': 'Product data not available',
        'category': 'N/A',
        'vendor_id': None,
        'unit_price': 0.0
    }])

    load_to_bq(dim_product, "dim_product")
    print(f"  ✓ dim_product: {len(dim_product)} rows loaded to BigQuery (placeholder)")
    return dim_product


# ─── Save to warehouse folder ─────────────────────────────────────────────────

def save_to_warehouse(dim_date, dim_customer, dim_vendor, dim_product):
    warehouse_dir = Path("warehouse/dimensions")
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    print("\nSaving dimension tables to warehouse folder...")

    dim_date.to_csv(warehouse_dir / "dim_date.csv", index=False)
    print(f"  ✓ dim_date.csv ({len(dim_date)} rows)")

    if not dim_customer.empty:
        dim_customer.to_csv(warehouse_dir / "dim_customer.csv", index=False)
        print(f"  ✓ dim_customer.csv ({len(dim_customer)} rows)")

    if not dim_vendor.empty:
        dim_vendor.to_csv(warehouse_dir / "dim_vendor.csv", index=False)
        print(f"  ✓ dim_vendor.csv ({len(dim_vendor)} rows)")

    dim_product.to_csv(warehouse_dir / "dim_product.csv", index=False)
    print(f"  ✓ dim_product.csv ({len(dim_product)} rows)")

    print(f"\nDimension tables saved to: {warehouse_dir.absolute()}")


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("DIMENSION TABLE POPULATION")
    print("=" * 60)
    print()
    print("Loading to BigQuery...")
    print()

    dim_date = populate_dim_date()
    dim_customer = populate_dim_customer()
    dim_vendor = populate_dim_vendor()
    dim_product = populate_dim_product()

    save_to_warehouse(dim_date, dim_customer, dim_vendor, dim_product)

    print()
    print("=" * 60)
    print("✓ All dimension tables populated")
    print("=" * 60)