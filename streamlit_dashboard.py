import streamlit as st
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Function to connect to MongoDB and fetch data
def get_mongodb_data():
    # Replace the placeholders with your MongoDB connection details
    
    mongodb_uri = 'mongodb+srv://DE-Mente:asdasdasd@cluster0.em28so6.mongodb.net'
    database_name='mydatabase'
    collection_name = 'mycollection'

    client = MongoClient(mongodb_uri)

    # Replace 'your_collection' with the name of your MongoDB collection
    collection = client[database_name][collection_name]

    # Fetch data from MongoDB and convert it to a DataFrame
    cursor = collection.find()
    data = list(cursor)
    df = pd.DataFrame(data)
    print("Data Fetched", df.shape)
    
    # eliminate duplicates, or eliminate the index column... !?
    df.drop('_id', axis=1, errors='ignore', inplace=True)
    #df.drop(['business_zip', 'location'], axis=1, inplace=True)
    #df = df.drop_duplicates()
    df = df[['ttxid', 'neighborhoods_analysis_boundaries']]
    
    print("Data preprocessed", df.shape)
    
    print("Data: ", df.head(), df.columns)
    
    # Close the MongoDB connection
    client.close()

    return df

# Streamlit App
def main():
    st.title("MongoDB Dashboard")

    # Get data from MongoDB
    df = get_mongodb_data()

    # Display the raw data
    st.subheader("Raw Data")
    
    # Display a plot (you can customize this based on your data)
    st.subheader("Plot")
    # Count plot for the 'business_zip' column
    st.bar_chart(df['neighborhoods_analysis_boundaries'].value_counts())
    
    # Display a table
    st.subheader("Table")
    st.table(df)
    #st.write("Data Types:", df.dtypes)

    

    

if __name__ == "__main__":
    main()