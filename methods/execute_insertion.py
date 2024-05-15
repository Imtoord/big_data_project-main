# Function to execute insertion of data

# Function to check if a string can be converted to a float
def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def execute_insertion(db, collection_name, data):
    collection = db[collection_name]
    try:
        # Convert numeric values to the appropriate data type
        data = {key: int(value) if value.isdigit() else float(value) if is_float(value) else value for key, value in data.items()}
        collection.insert_one(data)
        print("Data inserted successfully!")
        return True
    except Exception as e:
        print(f"An error occurred during insertion: {str(e)}")
        return False
