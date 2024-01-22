import sqlite3
import pandas as pd

def create_connection(database_path):
    try:
        connection = sqlite3.connect(database_path)
        return connection
    except sqlite3.Error as e:
        print(f"Error creating database connection: {e}")
        return None

def check_database_existence(database_path):
    try:
        with open(database_path):
            return True
    except FileNotFoundError:
        return False

def create_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS myTable (
                UniqueID TEXT PRIMARY KEY,
                Column1 TEXT,
                Column2 TEXT,
                -- Add more columns as needed
            );
        ''')
        connection.commit()
        cursor.close()
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def upload_data(connection, dataframe):
    try:
        # Check if the UniqueID already exists in the table
        existing_ids = pd.read_sql_query('SELECT UniqueID FROM myTable', connection)
        new_data = dataframe[~dataframe['UniqueID'].isin(existing_ids['UniqueID'])]

        if not new_data.empty:
            new_data.to_sql('myTable', connection, if_exists='append', index=False)
            print(f"Data uploaded successfully. New records: {len(new_data)}")
        else:
            print("No new records to upload.")
    except sqlite3.Error as e:
        print(f"Error uploading data: {e}")

def main():
    database_path = 'my_database.db'  # Change this to my desired database path
    cleaned_dataframe = pd.read_csv('cleaned_data.csv')  # Change this to my cleaned DataFrame

    connection = create_connection(database_path)

    if connection:
        if not check_database_existence(database_path):
            print("Database does not exist. Creating...")
            create_table(connection)
        else:
            print("Database already exists.")

        upload_data(connection, cleaned_dataframe)

        # Close the connection
        connection.close()

if __name__ == "__main__":
    main()
