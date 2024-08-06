import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

class MongoDBManager:
    def __init__(self):
        load_dotenv()
        uri = os.getenv("MONGODB_URI")
        db_name = os.getenv("DB_NAME")
        
        try:
            self.client = MongoClient(uri)
            self.db = self.client[db_name]
            print(f"Connected to MongoDB database: {db_name}")
        except ConnectionFailure as e:
            print(f"Could not connect to MongoDB: {e}")

    def insert_document(self, collection_name, document):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(collection_name)
            print(f"Collection {collection_name} created.")
        
        collection = self.db[collection_name]
        result = collection.insert_one(document)
        print(f"Document inserted with id {result.inserted_id}")

    def insert_documents(self, collection_name, documents):
        if collection_name not in self.db.list_collection_names():
            self.db.create_collection(collection_name)
            print(f"Collection {collection_name} created.")
        
        collection = self.db[collection_name]
        result = collection.insert_many(documents)
        print(f"Documents inserted with ids {result.inserted_ids}")
        
    def find_latest_document(self, collection_name):
        if collection_name not in self.db.list_collection_names():
            print(f"Collection {collection_name} does not exist.")
            return None
        
        collection = self.db[collection_name]
        latest_document = collection.find_one(sort=[("timestamp", -1)])
        
        if latest_document:
            print("Latest document found:")
            return latest_document
        else:
            print("No documents found in the collection.")
            return None

    def close_connection(self):
        self.client.close()
        print("MongoDB connection closed.")