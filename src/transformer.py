import pandas as pd
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
import os
import json
import hashlib
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = "data/transformed"
CHECKPOINT_FILE = "data/transformer_checkpoint.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─── MongoDB Helpers ────────────────────────────────────────────────────────

def get_db():
    client = MongoClient(os.getenv("MONGO_URI"))
    return client[os.getenv("MONGO_DB")]

def get_raw_collection():
    return get_db()["events_raw"]

def get_transformed_collection(name: str):
    return get_db()[name]


# ─── Checkpoint ──────────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


# ─── Extract ─────────────────────────────────────────────────────────────────

def extract_events(event_type_prefix: str, last_processed_at: str = None) -> pd.DataFrame:
    collection = get_raw_collection()
    query = {"event_type": {"$regex": f".*{event_type_prefix}.*", "$options": "i"}}
    if last_processed_at:
        query["ingested_at"] = {"$gt": last_processed_at}
    docs = list(collection.find(query, {"_id": 0}))
    print(f"  📦 Pulled {len(docs)} {event_type_prefix} events from MongoDB")
    return pd.DataFrame(docs) if docs else pd.DataFrame()


# ─── Field Normalizer ────────────────────────────────────────────────────────

def normalize_field(df: pd.DataFrame, possible_names: list, standard_name: str):
    for name in possible_names:
        if name in df.columns:
            df[standard_name] = df[name]
            return df
    df[standard_name] = None
    return df


# ─── Transform Functions ─────────────────────────────────────────────────────

def transform_orders(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    payload_df = pd.json_normalize(raw_df["payload"])

    payload_df = normalize_field(payload_df, ["order_id", "orderId", "id"], "order_id")
    payload_df = normalize_field(payload_df, ["customer_id", "customerId", "cust_id"], "customer_id")
    payload_df = normalize_field(payload_df, ["vendor_id", "vendorId", "vendor"], "vendor_id")
    payload_df = normalize_field(payload_df, ["status", "order_status", "state"], "status")
    payload_df = normalize_field(payload_df, ["amount", "total", "gross_amount", "total_amount"], "gross_amount")
    payload_df = normalize_field(payload_df, ["currency", "currency_code"], "currency")
    payload_df = normalize_field(payload_df, ["created_at", "order_date", "createdAt"], "created_at")
    payload_df = normalize_field(payload_df, ["updated_at", "updatedAt", "modified_at"], "updated_at")

    if "vendor_id" not in payload_df.columns or payload_df["vendor_id"].isna().all():
        payload_df["vendor_id"] = raw_df["vendor"].values

    output = payload_df[["order_id", "customer_id", "vendor_id", "status",
                          "gross_amount", "currency", "created_at", "updated_at"]].copy()

    output["created_at"] = pd.to_datetime(output["created_at"], errors="coerce", utc=True)
    output["updated_at"] = pd.to_datetime(output["updated_at"], errors="coerce", utc=True)
    output["_ingested_at"] = datetime.now(timezone.utc)
    output = output.dropna(subset=["order_id"])
    output = output.drop_duplicates(subset=["order_id"], keep="last").reset_index(drop=True)

    print(f"  ✅ Transformed {len(output)} orders")
    return output


def transform_payments(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    payload_df = pd.json_normalize(raw_df["payload"])

    payload_df = normalize_field(payload_df, ["payment_id", "paymentId", "transaction_id", "id"], "payment_id")
    payload_df = normalize_field(payload_df, ["order_id", "orderId", "orderRef"], "order_id")
    payload_df = normalize_field(payload_df, ["status", "payment_status", "state"], "status")
    payload_df = normalize_field(payload_df, ["amount", "amountPaid", "payment_amount", "total"], "amount")
    payload_df = normalize_field(payload_df, ["currency", "currencyCode", "currency_code"], "currency")
    payload_df = normalize_field(payload_df, ["payment_method", "method", "channel"], "payment_method")
    payload_df = normalize_field(payload_df, ["attempted_at", "paid_at", "payment_date", "created_at", "createdAt"], "attempted_at")

    payload_df["vendor_id"] = raw_df["vendor"].values

    output = payload_df[["payment_id", "order_id", "vendor_id", "status",
                          "amount", "currency", "payment_method", "attempted_at"]].copy()

    output["attempted_at"] = pd.to_datetime(output["attempted_at"], errors="coerce", utc=True)
    output["_ingested_at"] = datetime.now(timezone.utc)
    output = output.dropna(subset=["payment_id"])
    output = output.drop_duplicates(subset=["payment_id"], keep="last").reset_index(drop=True)

    print(f"  ✅ Transformed {len(output)} payments")
    return output


def transform_refunds(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    payload_df = pd.json_normalize(raw_df["payload"])

    payload_df = normalize_field(payload_df, ["refund_id", "refundId", "id"], "refund_id")
    payload_df = normalize_field(payload_df, ["order_id", "orderId", "orderRef"], "order_id")
    payload_df = normalize_field(payload_df, ["payment_id", "paymentId"], "payment_id")
    payload_df = normalize_field(payload_df, ["amount", "refund_amount"], "amount")
    payload_df = normalize_field(payload_df, ["reason", "refund_reason", "description"], "reason")
    payload_df = normalize_field(payload_df, ["refunded_at", "refundedAt", "refund_date", "created_at"], "refunded_at")
    payload_df = normalize_field(payload_df, ["currency", "currency_code"], "currency")

    payload_df["vendor_id"] = raw_df["vendor"].values

    if payload_df["refund_id"].isna().all():
        payload_df["refund_id"] = payload_df.apply(
            lambda r: hashlib.md5(f"{r.get('order_id','')}{r.get('refunded_at','')}".encode()).hexdigest(),
            axis=1
        )

    output = payload_df[["refund_id", "order_id", "payment_id", "vendor_id",
                          "amount", "currency", "reason", "refunded_at"]].copy()

    output["refunded_at"] = pd.to_datetime(output["refunded_at"], errors="coerce", utc=True)
    output["_ingested_at"] = datetime.now(timezone.utc)
    output = output.dropna(subset=["refund_id"])
    output = output.drop_duplicates(subset=["refund_id"], keep="last").reset_index(drop=True)

    print(f"  ✅ Transformed {len(output)} refunds")
    return output


def transform_shipments(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    payload_df = pd.json_normalize(raw_df["payload"])

    payload_df = normalize_field(payload_df, ["shipment_id", "shipmentId", "tracking", "id"], "shipment_id")
    payload_df = normalize_field(payload_df, ["order_id", "orderId", "orderRef"], "order_id")
    payload_df = normalize_field(payload_df, ["status", "shipment_status", "state"], "status")
    payload_df = normalize_field(payload_df, ["carrier", "courier", "shipping_carrier"], "carrier")

    payload_df["vendor_id"] = raw_df["vendor"].values

    def get_status_time(updates, status_name):
        if not isinstance(updates, list):
            return None
        for u in updates:
            if isinstance(u, dict) and u.get("status", "").upper() == status_name.upper():
                return u.get("time")
        return None

    if "updates" in payload_df.columns:
        payload_df["dispatched_at"] = payload_df["updates"].apply(
            lambda u: get_status_time(u, "PICKED_UP") or get_status_time(u, "CREATED")
        )
        payload_df["delivered_at"] = payload_df["updates"].apply(
            lambda u: get_status_time(u, "DELIVERED")
        )
        payload_df["status"] = payload_df["updates"].apply(
            lambda u: u[-1]["status"] if isinstance(u, list) and u else None
        )
    else:
        payload_df = normalize_field(payload_df, ["dispatched_at", "dispatch_date", "shipped_at"], "dispatched_at")
        payload_df = normalize_field(payload_df, ["delivered_at", "delivery_date", "completed_at"], "delivered_at")

    output = payload_df[["shipment_id", "order_id", "vendor_id", "status",
                          "carrier", "dispatched_at", "delivered_at"]].copy()

    output["dispatched_at"] = pd.to_datetime(output["dispatched_at"], errors="coerce", utc=True)
    output["delivered_at"] = pd.to_datetime(output["delivered_at"], errors="coerce", utc=True)
    output["_ingested_at"] = datetime.now(timezone.utc)
    output = output.dropna(subset=["shipment_id"])
    output = output.drop_duplicates(subset=["shipment_id"], keep="last").reset_index(drop=True)

    print(f"  ✅ Transformed {len(output)} shipments")
    return output


# ─── Save: MongoDB ────────────────────────────────────────────────────────────

def sanitize_for_mongo(df: pd.DataFrame) -> list:
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(object).where(df[col].notna(), None)
    return df.where(pd.notnull(df), None).to_dict("records")


def save_to_mongo(df: pd.DataFrame, collection_name: str, id_field: str):
    if df.empty:
        return

    collection = get_transformed_collection(collection_name)
    records = sanitize_for_mongo(df)

    operations = [
        UpdateOne(
            {id_field: record[id_field]},
            {"$set": record},
            upsert=True
        )
        for record in records
    ]

    result = collection.bulk_write(operations)
    print(f"  💾 MongoDB '{collection_name}': {result.upserted_count} inserted, {result.modified_count} updated")


# ─── Save: Local CSV ─────────────────────────────────────────────────────────

def save_to_csv(df: pd.DataFrame, filename: str):
    if df.empty:
        return

    filepath = os.path.join(OUTPUT_DIR, filename)
    write_header = not os.path.exists(filepath)
    df.to_csv(filepath, mode="a", header=write_header, index=False)
    print(f"  📄 CSV saved/updated: {filepath}")


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🔄 Starting incremental transformation...\n")

    checkpoint = load_checkpoint()
    now = datetime.now(timezone.utc).isoformat()

    # ── Orders ──
    print("📂 Orders:")
    orders_raw = extract_events("order", checkpoint.get("orders"))
    orders = transform_orders(orders_raw)
    save_to_mongo(orders, "orders_transformed", "order_id")
    save_to_csv(orders, "orders.csv")

    # ── Payments ──
    print("\n📂 Payments:")
    payments_raw = extract_events("payment", checkpoint.get("payments"))
    payments = transform_payments(payments_raw)
    save_to_mongo(payments, "payments_transformed", "payment_id")
    save_to_csv(payments, "payments.csv")

    # ── Refunds ──
    print("\n📂 Refunds:")
    refunds_raw = extract_events("refund", checkpoint.get("refunds"))
    refunds = transform_refunds(refunds_raw)
    save_to_mongo(refunds, "refunds_transformed", "refund_id")
    save_to_csv(refunds, "refunds.csv")

    # ── Shipments ──
    print("\n📂 Shipments:")
    shipments_raw = extract_events("shipment", checkpoint.get("shipments"))
    shipments = transform_shipments(shipments_raw)
    save_to_mongo(shipments, "shipments_transformed", "shipment_id")
    save_to_csv(shipments, "shipments.csv")

    # ── Load to BigQuery ──
    print("\n📂 Loading to BigQuery...")
    from bq_loader import (
        get_bq_client, upsert_orders, load_payments,
        load_refunds, load_shipments
    )

    bq = get_bq_client()
    upsert_orders(bq, orders)
    load_payments(bq, payments)
    load_refunds(bq, refunds)
    load_shipments(bq, shipments)

    # ── Save checkpoint ──
    save_checkpoint({
        "orders": now,
        "payments": now,
        "refunds": now,
        "shipments": now
    })

    print("\n✅ Transformation complete. Checkpoint saved.")