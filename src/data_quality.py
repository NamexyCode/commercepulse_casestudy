import pandas as pd
from datetime import datetime, timezone
from pymongo import MongoClient
from google.cloud import bigquery
import os
import json
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET")

def get_mongo():
    client = MongoClient(os.getenv("MONGO_URI"))
    return client[os.getenv("MONGO_DB")]["events_raw"]

def get_bq():
    return bigquery.Client(project=PROJECT)

def run_quality_checks():
    mongo = get_mongo()
    bq = get_bq()
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checks": {}
    }

    # 1. Count events by type in MongoDB
    pipeline = [{"$group": {"_id": "$event_type", "count": {"$sum": 1}}}]
    event_counts = {doc["_id"]: doc["count"] for doc in mongo.aggregate(pipeline)}
    report["checks"]["events_by_type"] = event_counts

    # 2. Orders with no matching payment
    no_payment_query = f"""
    SELECT COUNT(*) AS count
    FROM `{PROJECT}.{DATASET}.fact_orders` o
    LEFT JOIN `{PROJECT}.{DATASET}.fact_payments` p ON o.order_id = p.order_id
    WHERE o.status = 'paid' AND p.payment_id IS NULL
    """
    try:
        result = bq.query(no_payment_query).to_dataframe()
        report["checks"]["paid_orders_without_payment"] = int(result["count"][0])
    except Exception as e:
        report["checks"]["paid_orders_without_payment"] = f"Error: {e}"

    # 3. Refunds with no matching order
    orphan_refunds_query = f"""
    SELECT COUNT(*) AS count
    FROM `{PROJECT}.{DATASET}.fact_refunds` r
    LEFT JOIN `{PROJECT}.{DATASET}.fact_orders` o ON r.order_id = o.order_id
    WHERE o.order_id IS NULL
    """
    try:
        result = bq.query(orphan_refunds_query).to_dataframe()
        report["checks"]["refunds_without_orders"] = int(result["count"][0])
    except Exception as e:
        report["checks"]["refunds_without_orders"] = f"Error: {e}"

    # 4. Payment success rate by vendor
    payment_rate_query = f"""
    SELECT
        vendor_id,
        ROUND(COUNTIF(status='success') / COUNT(*) * 100, 2) AS success_rate_pct
    FROM `{PROJECT}.{DATASET}.fact_payments`
    GROUP BY vendor_id
    ORDER BY success_rate_pct ASC
    """
    try:
        result = bq.query(payment_rate_query).to_dataframe()
        report["checks"]["payment_success_rate_by_vendor"] = result.to_dict(orient="records")
    except Exception as e:
        report["checks"]["payment_success_rate_by_vendor"] = f"Error: {e}"

    # 5. Late refunds (refund arrived after order was reported as complete)
    late_refunds_query = f"""
    SELECT COUNT(*) AS count
    FROM `{PROJECT}.{DATASET}.fact_refunds` r
    JOIN `{PROJECT}.{DATASET}.fact_orders` o ON r.order_id = o.order_id
    WHERE r.refunded_at > o.updated_at
    """
    try:
        result = bq.query(late_refunds_query).to_dataframe()
        report["checks"]["late_refunds"] = int(result["count"][0])
    except Exception as e:
        report["checks"]["late_refunds"] = f"Error: {e}"

    # Save report to file
    date_str = datetime.now().strftime("%Y-%m-%d")
    os.makedirs("reports", exist_ok=True)
    report_path = f"reports/dq_report_{date_str}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"📊 Data quality report saved to {report_path}")
    return report

if __name__ == "__main__":
    run_quality_checks()