# Load Data into SQLite Database

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database schema
Base = declarative_base()

class Business(Base):
    __tablename__ = 'business'
    BusinessID = Column(Integer, primary_key=True)
    BusinessAccountNumber = Column(String)
    OwnershipName = Column(String)
    # ... add other columns as needed

class Location(Base):
    __tablename__ = 'location'
    LocationID = Column(Integer, primary_key=True)
    BusinessID = Column(Integer, ForeignKey('business.BusinessID'))
    StreetAddress = Column(String)
    City = Column(String)
    State = Column(String)
    # ... add other columns as needed

class Neighborhood(Base):
    __tablename__ = 'neighborhood'
    NeighborhoodID = Column(Integer, primary_key=True)
    NeighborhoodName = Column(String)
    # ... add other columns as needed

# Database URL (SQLite in this example)
db_url = 'sqlite:///my_database.db'

# Create the engine and tables
engine = create_engine(db_url)
Base.metadata.create_all(engine)

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Example DataFrame (replace this with my actual cleaned DataFrame)
df_cleaned = pd.DataFrame({
    'BusinessAccountNumber': ['123', '456'],
    'OwnershipName': ['Owner1', 'Owner2'],
    # Add more columns as needed
})

if not df_cleaned.empty:
    try:
        # Add data to the session and commit to the database
        session.bulk_insert_mappings(Business, df_cleaned.to_dict(orient='records'))
        session.commit()
        print("Data inserted successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error inserting data: {e}")
    finally:
        session.close()
else:
    print("No data to upload to the database.")




#=========================================================================


# Use Example:
# Create an SQLite database (I can replace this with another RDBMS)
# Example DataFrame (replace this with my actual cleaned DataFrame)
#df_cleaned = pd.DataFrame({
#    'BusinessAccountNumber': ['123', '456'],
#    'OwnershipName': ['Owner1', 'Owner2'],
#    # Add more columns as needed
#})

# Create and use the database
#engine = create_engine('sqlite:///my_database.db')

# Create a session to interact with the database
#Session = sessionmaker(bind=engine)
#session = Session()

#if not df_cleaned.empty:
#    try:
        # Add data to the session and commit to the database
#        session.bulk_insert_mappings(Business, df_cleaned.to_dict(orient='records'))
#        session.commit()
#        print("Data inserted successfully.")
#    except Exception as e:
#        session.rollback()
#        print(f"Error inserting data: {e}")
#    finally:
#        session.close()
#else:
#    print("No data to upload to the database.")
