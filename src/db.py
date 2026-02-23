import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

# Load .env at module level - THIS RUNS WHEN THE MODULE IS IMPORTED
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Debug - verify loading happened
print(f"📚 db.py: Loading .env from {env_path}")
print(f"📚 db.py: MONGO_URI loaded: {'Yes' if os.getenv('MONGO_URI') else 'No'}")

def get_mongo_collection():
    """Get the events_raw collection from MongoDB"""
    
    # Get environment variables
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    
    # Verify they exist
    if not mongo_uri:
        # Try loading again just in case
        load_dotenv(dotenv_path=env_path, override=True)
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB")
        
        if not mongo_uri:
            raise ValueError("❌ MONGO_URI not found in environment variables")
    
    print(f"✅ Connected to MongoDB Atlas")
    
    try:
        # Create client with SSL certificates
        client = MongoClient(
            mongo_uri,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=10000
        )
        
        # Test connection
        client.admin.command('ping')
        
        # Get database and collection
        db = client[db_name]
        collection = db["events_raw"]
        
        return collection
        
    except Exception as e:
        print(f"❌ MongoDB connection error: {e}")
        raise