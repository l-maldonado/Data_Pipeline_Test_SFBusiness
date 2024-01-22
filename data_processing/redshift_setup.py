import psycopg2

def create_redshift_cluster():
    # Code to create Redshift cluster
    
    redshift = psycopg2.connect(
        dbname='MyDatabaseName',
        user='MyMasterUsername',
        password='<MasterPassword',
        host='MyClusterEndpoint',
        port='5439'
    )

    cursor = redshift.cursor()

    # Create a database
    cursor.execute('CREATE DATABASE YourDatabaseName')

    # Commit changes and close connections
    redshift.commit()
    cursor.close()
    redshift.close()

if __name__ == "__main__":
    create_redshift_cluster()
