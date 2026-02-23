import json
import os
import glob
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()

def get_collection():
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("MONGO_DB")]
    return db["events_raw"]

def load_live_events(date_folder: str, collection):
    """
    Load all events from a single day's events.jsonl file.
    Uses upsert so re-running is safe (idempotent).
    """
    filepath = os.path.join(date_folder, "events.jsonl")
    if not os.path.exists(filepath):
        print(f"  ⚠️  No events file found at {filepath}")
        return

    inserted = 0
    skipped = 0

    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            event = json.loads(line)

            # Live events should already have event_id from the generator
            if "event_id" not in event:
                print(f"  ⚠️  Event missing event_id, skipping: {line[:80]}")
                continue

            event["ingested_at"] = datetime.now(timezone.utc).isoformat()

            try:
                collection.update_one(
                    {"event_id": event["event_id"]},   # find by ID
                    {"$setOnInsert": event},            # only insert if not found
                    upsert=True                         # create if doesn't exist
                )
                inserted += 1
            except Exception as e:
                print(f"  ❌ Error inserting event {event.get('event_id')}: {e}")
                skipped += 1

    print(f"  {date_folder}: {inserted} processed, {skipped} errors")

def run_ingestion():
    collection = get_collection()
    live_dir = "data/live_events"

    # Find all date folders
    date_folders = sorted(glob.glob(os.path.join(live_dir, "*")))

    print(f"Ingesting live events from {len(date_folders)} date folder(s)...")
    for folder in date_folders:
        load_live_events(folder, collection)

    print("✅ Live event ingestion complete.")

if __name__ == "__main__":
    run_ingestion()