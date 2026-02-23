from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

def test_atlas_connection():
    try:
        client = MongoClient(os.getenv("MONGO_URI"))
        
        # This forces an actual connection attempt
        client.admin.command("ping")
        print("✅ MongoDB Atlas connection successful!")
        
        # Create the database and collection
        db = client[os.getenv("MONGO_DB")]
        collection = db["events_raw"]
        
        # Insert a test document
        result = collection.insert_one({"test": "hello from atlas", "status": "connection_test"})
        print(f"✅ Test document inserted with ID: {result.inserted_id}")
        
        # Read it back
        doc = collection.find_one({"test": "hello from atlas"})
        print(f"✅ Test document retrieved: {doc}")
        
        # Clean up the test document
        collection.delete_one({"test": "hello from atlas"})
        print("✅ Test document cleaned up")
        
        print("\n🎉 Atlas is fully working! You're ready to proceed.")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Double-check your password in .env — no < > brackets around it")
        print("  2. Make sure your IP is whitelisted in Atlas Network Access")
        print("  3. Make sure you installed pymongo[srv]: pip install 'pymongo[srv]'")

if __name__ == "__main__":
    test_atlas_connection()