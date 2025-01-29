import pandas as pd
from pymongo import MongoClient
        
# Assuming df is the pandas DataFrame containing the dataset, provided as an argument to the method.

# If local installation:
mongo_host = 'localhost'
mongo_port = 27017

# otherwise, using the connection string:
mongodb_uri = 'mongodb://localhost:27017/'

mongo_uri = 'mongodb+srv://DE-Mente:asdasdasd@cluster.mongodb.net/'

connection_uri = 'mongodb+srv://<username>:<password>@<cluster>/<database>?retryWrites=true&w=majority'
connection_uri = 'mongodb+srv://DE-Mente:asdasdasd@cluster.mongodb.net/mydatabase?retryWrites=true&w=majority'


def upload_to_mongodb(dataframe, connection_uri, collection_name = 'mycollection', database_name='mydatabase'):    
    """
    Uploads a Pandas DataFrame to a MongoDB collection.

    Parameters:
    - dataframe: Pandas DataFrame to be uploaded.
    - collection_name: Name of the MongoDB collection (default is 'my_collection_name'.
    - database_name: Name of the MongoDB database (default is 'my_database').
    - host: MongoDB host address (default is 'localhost').
    - port: MongoDB port (default is 27017).
    - connection_uri: MongoDB connection URI.
    """
    
    if dataframe is not None:
        # Connect to MongoDB
        client = MongoClient(connection_uri)
        # Alternatively, for a local mongo installation: client = MongoClient(mongodb_uri)
           
        try:
            db = client[database_name]
            collection = db[collection_name]
 
            # Convert DataFrame to a list of dictionaries
            data = dataframe.to_dict(orient='records')

            # Insert data into MongoDB collection
            collection.insert_many(data)
            print("Data uploaded to MongoDB collection '{collection_name}' in database '{database_name}' successfully.")
        
        except Exception as e:
            print(f"Error uploading data to MongoDB: {e}")
        
        finally:
            # Close MongoDB connection
            client.close()
            
            
    else:
        print("No data to upload to MongoDB.")


        
#=========================================================================
# Example of Use:
#if __name__ == "__main__":
# Example DataFrame
#    df_example = pd.DataFrame({
#        'Name': ['John', 'Alice'],
#        'Age': [25, 30],
#        'City': ['New York', 'San Francisco']
#    })
#
# Example usage of the module
#    upload_to_mongodb(df_example, collection_name='example_collection', database_name='example_db')