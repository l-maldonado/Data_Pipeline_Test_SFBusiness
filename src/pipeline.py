from data_fetcher import fetch_data
from data_cleaner import clean_transform_data
from mongodb_uploader import upload_to_mongodb

from data_ingestion.dynamodb_setup import create_dynamodb_table
from data_ingestion.dynamodb_insert import insert_data_into_dynamodb
from data_processing.redshift_setup import create_redshift_cluster
from data_processing.redshift_insert import insert_data_into_redshift

import subprocess
import time
import os
import signal

api_url = "https://data.sfgov.org/resource/g8m3-pdis.json"

# Example query 
query = "limit=100"

mongodb_uri = 'mongodb+srv://DE-Mente:asdasdasd@cluster.mongodb.net'
database_name='mydatabase'
collection_name = 'mycollection'

def data_pipeline(api_url, query, mongodb_uri, database_name, collection_name):
    
    
    # Step 1: Fetch Data
    data = fetch_data(api_url, query)

    
    
    
    # Step 2: Clean and Transform Data
    df = clean_transform_data(data)

    
    
    
    # Step 3: Upload to MongoDB
    # MongoDB connection details
    upload_to_mongodb(df, mongodb_uri, collection_name, database_name)   
    
    # Alternatives considered:
    # Data Ingestion Phase
    #create_dynamodb_table()
    #insert_data_into_dynamodb()

    # Data Processing Phase
    #create_redshift_cluster()
    #insert_data_into_redshift()
    
    
    
    
    
    # Step 4: Show  a dashboard in Streamlit, wait for some time and then terminate the process. 
    try:
        # Define the command to run the Streamlit app
        streamlit_command = "streamlit run streamlit_dashboard.py"

        # Start the Streamlit app using subprocess
        streamlit_process = subprocess.Popen(streamlit_command, shell=True, preexec_fn=os.setsid)

        # Do other tasks or operations in the pipeline

        # Optionally, wait for some time
        time.sleep(30)  # tuned for 30 seconds. Adjust the duration as needed

        # Terminate the Streamlit process
        streamlit_process.terminate()
    
    except Exception as e:
        print(f"Error runing streamlit Dashboard: {e}")
        
    finally:
        # Close process by killing it
        os.killpg(os.getpgid(streamlit_process.pid), signal.SIGTERM)
    

if __name__ == "__main__":
    import sys
    
    # Extract command line arguments
    args = sys.argv[1:]

    # Check if the method should be called
    if args[0] == "data_pipeline":
        # Call the fetch_data method with the provided argument
        result = data_pipeline(args[1])
        print(result)
    

#===========================================================
# Example Usage:
#api_url = 'your_api_endpoint_url'
#mongodb_uri = 'mongodb://localhost:27017/'
#db_name = 'your_database_name'
#collection_name = 'your_collection_name'

#data_pipeline(api_url, mongodb_uri, db_name, collection_name)


