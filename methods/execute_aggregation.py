
# Function to execute an aggregation query
def execute_aggregation(db,collection_name, pipeline):
    collection = db[collection_name]
    result = list(collection.aggregate(pipeline))
    return result
