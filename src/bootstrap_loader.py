import json
import hashlib
import os
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()

def get_collection():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB")]
    return db["events_raw"]

def make_event_id(source_file: str, record: dict) -> str:
    """
    Creates a deterministic (stable) ID by hashing the source file name
    + the full record content. Same input always = same ID.
    This means re-running the loader won't create duplicates.
    """
    raw = f"{source_file}:{json.dumps(record, sort_keys=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()

def infer_event_type(filename: str) -> str:
    """Map filename to an event type label."""
    mapping = {
        "orders_2023.json": "historical_order",
        "payments_2023.json": "historical_payment",
        "shipments_2023.json": "historical_shipment",
        "refunds_2023.json": "historical_refund",
    }
    return mapping.get(filename, "historical_unknown")

def infer_event_time(record: dict) -> str:
    """Try to extract a timestamp from the record, fall back to now."""
    for field in ["created_at", "order_date", "payment_date", "shipped_at", "refund_date"]:
        if field in record:
            return record[field]
    return datetime.now(timezone.utc).isoformat()

def load_bootstrap_file(filepath: str, collection):
    filename = os.path.basename(filepath)
    event_type = infer_event_type(filename)

    with open(filepath, "r") as f:
        records = json.load(f)  # loads the whole JSON array

    inserted = 0
    skipped = 0

    for record in records:
        event_id = make_event_id(filename, record)
        event_doc = {
            "event_id": event_id,
            "event_type": event_type,
            "event_time": infer_event_time(record),
            "vendor": record.get("vendor_id") or record.get("vendor") or "unknown",
            "payload": record,       # original record untouched
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            collection.insert_one(event_doc)
            inserted += 1
        except DuplicateKeyError:
            skipped += 1  # already exists, safe to skip

    print(f"  {filename}: {inserted} inserted, {skipped} skipped (duplicates)")

def run_bootstrap():
    collection = get_collection()
    bootstrap_dir = "data/bootstrap"

    files = [
        "orders_2023.json",
        "payments_2023.json",
        "shipments_2023.json",
        "refunds_2023.json",
    ]

    print("🚀 Starting historical bootstrap...")
    for filename in files:
        filepath = os.path.join(bootstrap_dir, filename)
        if os.path.exists(filepath):
            load_bootstrap_file(filepath, collection)
        else:
            print(f"  ⚠️  File not found: {filepath}")

    print("✅ Bootstrap complete.")

if __name__ == "__main__":
    run_bootstrap()