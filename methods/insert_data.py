from pymongo import MongoClient
import json

def insert_data_to_mongo(data_file, collection_name):
    with open(data_file, 'r') as f:
        data = json.load(f)
    client = MongoClient('mongodb://localhost:27017')
    db = client["schools"]

    collection = db[collection_name]
    collection.insert_many(data)
    client.close()
