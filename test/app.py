import streamlit as st
import pandas as pd

# Import methods
from methods.execute_query import execute_query
from methods.get_attributes import get_attributes
from methods.mongodb_connection import connect_to_mongodb
from methods.execute_insertion import execute_insertion  # Import the insertion method
from methods.execute_deletion import *  # Import the deletion method
from methods.execute_aggregation import execute_aggregation  # Import the aggregation method
from methods.get_aggregation_query import get_aggregation_query  # Import the aggregation query method
from methods.get_aggregation_query_names import get_aggregation_query_names  # Import the method to get aggregation query names

# Main function to define the Streamlit app
def main():
    st.title("Schools Data Explorer in USA")
    
    # Sidebar for selecting dataset
    dataset = st.sidebar.selectbox("Select Dataset", ["public", "private"])
    collection_name = dataset  # Collection name is same as dataset

    # Connect to MongoDB
    db = connect_to_mongodb()

    # Sidebar for selecting query type
    query_type = st.sidebar.radio("Select Query Type", ["Basic Query", "Insert", "Delete", "Aggregate"])  # Add "Aggregate" option

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

    elif query_type == "Insert":  # Add insertion logic here
        st.subheader("Insert Data")
        st.write(f"Inserting into collection: {collection_name}")
        data = {}
        for attribute in get_attributes(collection_name):
            if attribute != "_id":  # Exclude '_id' from insertion
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

    elif query_type == "Aggregate":  # Add aggregate logic here
        st.subheader("Aggregate Query")
        query_names = get_aggregation_query_names()
        selected_query = st.selectbox("Select Aggregate Query", query_names)
        
        if selected_query.startswith("Count Schools with Population Greater Than"):
            value = st.number_input("Enter Population Value", value=0)
            pipeline = get_aggregation_query(selected_query)(value)
        elif selected_query.startswith("Count Schools with Population Less Than"):
            value = st.number_input("Enter Population Value", value=0)
            pipeline = get_aggregation_query(selected_query)(value)
        elif selected_query.startswith("Count Schools with Enrollment Between"):
            min_val = st.number_input("Enter Minimum Enrollment", value=0)
            max_val = st.number_input("Enter Maximum Enrollment", value=0)
            pipeline = get_aggregation_query(selected_query)(min_val, max_val)
        elif selected_query.startswith("Average Population by State with Population Greater Than"):
            value = st.number_input("Enter Population Value", value=0)
            pipeline = get_aggregation_query(selected_query)(value)
        elif selected_query.startswith("Average Population by State with Population Less Than"):
            value = st.number_input("Enter Population Value", value=0)
            pipeline = get_aggregation_query(selected_query)(value)
        elif selected_query.startswith("Average Population by State with Enrollment Between"):
            min_val = st.number_input("Enter Minimum Enrollment", value=0)
            max_val = st.number_input("Enter Maximum Enrollment", value=0)
            pipeline = get_aggregation_query(selected_query)(min_val, max_val)
        else:
            pipeline = get_aggregation_query(selected_query)
        
        if st.button("Execute"):
            result = execute_aggregation(db, collection_name, pipeline)
            if result:
                st.write("Aggregate Query Result:")
                df = pd.DataFrame(result)
                st.write(df)
            else:
                st.write("No results found.")

if __name__ == "__main__":
    main()
