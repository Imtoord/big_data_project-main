from pymongo import MongoClient

def connect_to_mongodb():
# Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017')
    db = client["schools"]
    return db
