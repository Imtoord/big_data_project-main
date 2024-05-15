import streamlit as st
import pandas as pd
from pymongo import MongoClient
from bson import ObjectId  # Add import for ObjectId

# Connect to MongoDB
def connect_to_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["patient"]
    return db

# Method to execute a query
# Method to execute a query
def execute_query(db, collection_name, query):
    collection = db[collection_name]
    if "_id" in query:
        query["_id"] = ObjectId(query["_id"])
    result = list(collection.find(query))
    return result


# Method to get attributes
def get_attributes(collection_name):
    db = connect_to_mongodb()
    collection = db[collection_name]
    sample_doc = collection.find_one()
    if sample_doc:
        attributes = list(sample_doc.keys())
        return attributes
    else:
        return []

# Method to execute an insertion
def execute_insertion(db, collection_name, data):
    collection = db[collection_name]
    insertion_result = collection.insert_one(data)
    return insertion_result.acknowledged

# Method to execute a deletion
def execute_deletion(db, collection_name, document_id):
    collection = db[collection_name]
    deletion_result = collection.delete_one({"_id": ObjectId(document_id)})
    return deletion_result.deleted_count > 0

# Method to execute an aggregation
def execute_aggregation(db, collection_name, pipeline):
    collection = db[collection_name]
    result = list(collection.aggregate(pipeline))
    return result

# Main function to define the Streamlit app
def main():
    st.title("Hospital Data Explorer")

    # Sidebar for selecting dataset
    dataset = st.sidebar.selectbox("Select Dataset", ["Admissions Collection", "MedicalRecords Collection", "Patients Collection"])
    collection_name = dataset

    # Connect to MongoDB
    db = connect_to_mongodb()

    # Sidebar for selecting query type
    query_type = st.sidebar.radio("Select Query Type", ["Basic Query", "Insert", "Delete", "Aggregate"])

    if query_type == "Basic Query":
        st.subheader("Basic Query")

        # Get attributes based on collection name
        attributes = get_attributes(collection_name)
        attribute = st.selectbox("Select Attribute", attributes, index=0)

        value = st.text_input("Enter Value")

        query = {attribute: value}



        if st.button("Execute"):
            result = execute_query(db, collection_name, query)
            if result:
                st.write("Query Result:")
                df = pd.DataFrame(result)
                st.write(df)
            else:
                st.write("No results found.")

    elif query_type == "Insert":
        st.subheader("Insert Data")
        st.write(f"Inserting into collection: {collection_name}")
        data = {}
        for attribute in get_attributes(collection_name):
            if attribute != "_id":
                data[attribute] = st.text_input(attribute)
        if st.button("Insert"):
            if execute_insertion(db, collection_name, data):
                st.success("Data inserted successfully!")
            else:
                st.error("Failed to insert data. Please check your input.")

    elif query_type == "Delete":
        st.subheader("Delete Data")
        st.write(f"Deleting document from collection: {collection_name}")
        document_id = st.text_input("Enter Document ID")
        if st.button("Delete"):
            if execute_deletion(db, collection_name, document_id):
                st.success("Document deleted successfully!")
            else:
                st.error("Failed to delete document. Please check your input.")

    elif query_type == "Aggregate":
        st.subheader("Aggregate Query")
        st.write("Aggregation queries are not yet implemented for this dataset.")

if __name__ == "__main__":
    main()
