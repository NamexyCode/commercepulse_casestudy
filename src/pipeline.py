from transformer import (
    extract_events, transform_orders, transform_payments,
    transform_refunds, transform_shipments
)
from bq_loader import (
    get_bq_client, upsert_orders, load_payments,
    load_refunds, load_dataframe, compute_and_load_daily_summary
)
from data_quality import run_quality_checks
from datetime import datetime

def run_pipeline():
    print(f"\n{'='*50}")
    print(f"🚀 Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    bq = get_bq_client()

    # Step 1: Orders
    print("📦 Processing orders...")
    raw_orders = extract_events("order")
    clean_orders = transform_orders(raw_orders)
    upsert_orders(bq, clean_orders)

    # Step 2: Payments
    print("\n💳 Processing payments...")
    raw_payments = extract_events("payment")
    clean_payments = transform_payments(raw_payments)
    load_payments(bq, clean_payments)

    # Step 3: Refunds
    print("\n💸 Processing refunds...")
    raw_refunds = extract_events("refund")
    clean_refunds = transform_refunds(raw_refunds)
    load_refunds(bq, clean_refunds)

    # Step 4: Shipments
    print("\n🚚 Processing shipments...")
    raw_shipments = extract_events("shipment")
    clean_shipments = transform_shipments(raw_shipments)
    load_dataframe(bq, clean_shipments, "fact_shipments")

    # Step 5: Daily summary
    print("\n📊 Computing daily summary...")
    compute_and_load_daily_summary(bq)

    # Step 6: Data quality
    print("\n🔍 Running data quality checks...")
    run_quality_checks()

    print(f"\n{'='*50}")
    print("✅ Pipeline complete!")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    run_pipeline()