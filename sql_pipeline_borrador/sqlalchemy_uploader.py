import pandas as pd
from sqlalchemy import create_engine

def create_connection(database_path):
    try:
        engine = create_engine(f'sqlite:///{database_path}', echo=False)
        return engine
    except Exception as e:
        print(f"Error creating database connection: {e}")
        return None

def check_table_existence(engine, table_name):
    try:
        return engine.dialect.has_table(engine, table_name)
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False

def create_table(engine, table_name):
    try:
        engine.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                UniqueID TEXT PRIMARY KEY,
                Column1 TEXT,
                Column2 TEXT,
                -- Add more columns as needed
            );
        ''')
    except Exception as e:
        print(f"Error creating table: {e}")

def upload_data(engine, table_name, dataframe):
    try:
        # Check if the UniqueID already exists in the table
        existing_ids = pd.read_sql_query(f'SELECT UniqueID FROM {table_name}', engine)
        new_data = dataframe[~dataframe['UniqueID'].isin(existing_ids['UniqueID'])]

        if not new_data.empty:
            new_data.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Data uploaded successfully. New records: {len(new_data)}")
        else:
            print("No new records to upload.")
    except Exception as e:
        print(f"Error uploading data: {e}")

def main():
    database_path = 'my_database.db'  # Change this to my desired database path
    table_name = 'myTable'  # Change this to my desired table name
    cleaned_dataframe = pd.read_csv('cleaned_data.csv')  # Change this to my cleaned DataFrame

    engine = create_connection(database_path)

    if engine:
        if not check_table_existence(engine, table_name):
            print("Table does not exist. Creating...")
            create_table(engine, table_name)
        else:
            print("Table already exists.")

        upload_data(engine, table_name, cleaned_dataframe)

        # Dispose of the engine
        engine.dispose()

if __name__ == "__main__":
    main()
