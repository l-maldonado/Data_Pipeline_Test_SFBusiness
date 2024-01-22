import psycopg2

def insert_data_into_redshift():
    # Code to insert data into Redshift
    
    redshift = psycopg2.connect(
        dbname='MyDatabaseName',
        user='MyMasterUsername',
        password='MasterPassword',
        host='MyClusterEndpoint',
        port='5439'
    )

    cursor = redshift.cursor()

    # Insert data into Redshift table
    cursor.execute("INSERT INTO YourTable (Column1, Column2) VALUES ('Value1', 'Value2');")

    # Commit changes and close connections
    redshift.commit()
    cursor.close()
    redshift.close()

if __name__ == "__main__":
    insert_data_into_redshift()
