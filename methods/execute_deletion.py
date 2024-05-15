# Function to execute a deletion operation
def execute_deletion(db, collection_name, document_id):
    collection = db[collection_name]
    try:
        collection.delete_one({"_id": document_id})
        return True
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False
