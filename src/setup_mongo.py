#!/usr/bin/env python
"""
Setup MongoDB indexes for commercepulse
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

print("=" * 50)
print("SETTING UP MONGODB INDEXES")
print("=" * 50)

def get_collection():
    """Get the events_raw collection (matching your test pattern)"""
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    
    print(f"📁 Project root: {project_root}")
    print(f"📄 .env path: {env_path}")
    print(f"📄 .env exists: {env_path.exists()}")
    print(f"🔑 Using database: {db_name}")
    
    # Connect (exactly like your test)
    client = MongoClient(
        mongo_uri,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=5000
    )
    
    # Test connection
    client.admin.command('ping')
    print("✅ Connected to MongoDB Atlas")
    
    # Get collection
    db = client[db_name]
    return db["events_raw"]

def setup_indexes():
    """Create necessary indexes for events_raw collection"""
    try:
        print("\n🔄 Getting collection...")
        collection = get_collection()
        print(f"✅ Connected to collection: {collection.name}")
        print(f"✅ Database: {collection.database.name}")
        
        # Create indexes
        print("\n📊 Creating indexes...")
        
        # Index on timestamp for efficient sorting
        collection.create_index("timestamp")
        print("  ✅ Created index on 'timestamp'")
        
        # Optional: Create compound index for common queries
        try:
            collection.create_index([("event_type", 1), ("timestamp", -1)])
            print("  ✅ Created compound index on 'event_type' and 'timestamp'")
        except Exception as e:
            print(f"  ⚠️ Compound index not created: {e}")
        
        # List all indexes
        print("\n📋 Current indexes:")
        for index in collection.list_indexes():
            print(f"  - {index['name']}: {index['key']}")
            
        print("\n✅ Index setup complete!")
        
        # Test with a sample document
        test_doc = {
            "test": "index_setup",
            "timestamp": "2024-01-01T00:00:00Z",
            "event_type": "test"
        }
        result = collection.insert_one(test_doc)
        print(f"✅ Test document inserted with ID: {result.inserted_id}")
        collection.delete_one({"_id": result.inserted_id})
        print("✅ Test document cleaned up")
        
    except Exception as e:
        print(f"❌ Error setting up indexes: {e}")
        raise

if __name__ == "__main__":
    setup_indexes()