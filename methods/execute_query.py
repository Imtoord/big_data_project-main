def execute_query(db, collection_name, query):
    collection = db[collection_name]
    result = list(collection.find(query))
    
    return result
