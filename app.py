import streamlit as st
import pandas as pd
from pymongo import MongoClient
from bson import ObjectId

# Connect to MongoDB
def connect_to_mongodb():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["patient"]
    return db

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

# Method to execute an update
def execute_update(db, collection_name, document_id, update_data):
    collection = db[collection_name]
    update_result = collection.update_one({"_id": ObjectId(document_id)}, {"$set": update_data})
    return update_result.modified_count > 0

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
    query_type = st.sidebar.radio("Select Query Type", ["Basic Query", "Insert", "Delete", "Update", "Aggregate"])

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

    elif query_type == "Update":
        st.subheader("Update Data")
        st.write(f"Updating document in collection: {collection_name}")
        document_id = st.text_input("Enter Document ID")
        update_data = {}
        for attribute in get_attributes(collection_name):
            if attribute != "_id":
                new_value = st.text_input(f"Enter new value for {attribute} (leave blank to keep unchanged)")
                if new_value:
                    update_data[attribute] = new_value
        if st.button("Update"):
            if execute_update(db, collection_name, document_id, update_data):
                st.success("Document updated successfully!")
            else:
                st.error("Failed to update document. Please check your input.")

    elif query_type == "Aggregate":
        st.subheader("Aggregate Query")

        # Dropdown for selecting aggregation operation
        operation = st.selectbox("Select Aggregation Operation", [
            "Group by and count",
            "Group by and sum",
            "Group by and average",
            "Match",
            "Sort"
        ])

        if operation == "Group by and count":
            group_by_field = st.selectbox("Select field to group by", get_attributes(collection_name))
            pipeline = [
                {"$group": {"_id": f"${group_by_field}", "count": {"$sum": 1}}}
            ]

        elif operation == "Group by and sum":
            group_by_field = st.selectbox("Select field to group by", get_attributes(collection_name))
            sum_field = st.selectbox("Select numeric field to sum", get_attributes(collection_name))
            pipeline = [
                {"$group": {"_id": f"${group_by_field}", "total_sum": {"$sum": f"${sum_field}"}}}
            ]

        elif operation == "Group by and average":
            group_by_field = st.selectbox("Select field to group by", get_attributes(collection_name))
            avg_field = st.selectbox("Select numeric field to average", get_attributes(collection_name))
            pipeline = [
                {"$group": {"_id": f"${group_by_field}", "average": {"$avg": f"${avg_field}"}}}
            ]

        elif operation == "Match":
            match_field = st.selectbox("Select field to match", get_attributes(collection_name))
            match_value = st.text_input("Enter value to match")
            pipeline = [
                {"$match": {match_field: match_value}}
            ]

        elif operation == "Sort":
            sort_field = st.selectbox("Select field to sort by", get_attributes(collection_name))
            sort_order = st.selectbox("Select sort order", ["Ascending", "Descending"])
            sort_order_value = 1 if sort_order == "Ascending" else -1
            pipeline = [
                {"$sort": {sort_field: sort_order_value}}
            ]

        if st.button("Execute Aggregation"):
            result = execute_aggregation(db, collection_name, pipeline)
            if result:
                st.write("Aggregation Result:")
                df = pd.DataFrame(result)
                st.write(df)
            else:
                st.write("No results found.")

if __name__ == "__main__":
    main()
