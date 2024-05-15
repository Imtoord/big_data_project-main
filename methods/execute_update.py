
# Function to execute an update operation
def execute_update(db ,ObjectId, collection_name, document_id, updated_fields):
    collection = db[collection_name]
    try:
        # Convert numeric values to the appropriate data type
        updated_fields = {key: int(value) if value.isdigit() else float(value) if is_float(value) else value for key, value in updated_fields.items()}
        collection.update_one({"_id": ObjectId(document_id)}, {"$set": updated_fields})
        return True
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False
