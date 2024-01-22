# Using SQLAlchemy to create a connection to the SQL database and upload the cleaned DataFrame.

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
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

def create_database():
    # Database URL (SQLite in this example)
    db_url = 'sqlite:///my_database.db'

    # Create the engine and tables
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    return engine

def insert_data(engine):
    # Create a session to interact with the database
    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert example data
    business_data = [
        Business(BusinessAccountNumber='123', OwnershipName='Owner1'),
        Business(BusinessAccountNumber='456', OwnershipName='Owner2')
        # Add more Business instances as needed
    ]

    location_data = [
        Location(BusinessID=1, StreetAddress='Street1', City='City1', State='State1'),
        Location(BusinessID=2, StreetAddress='Street2', City='City2', State='State2')
        # Add more Location instances as needed
    ]

    neighborhood_data = [
        Neighborhood(NeighborhoodName='Neighborhood1'),
        Neighborhood(NeighborhoodName='Neighborhood2')
        # Add more Neighborhood instances as needed
    ]

    try:
        # Add data to the session and commit to the database
        session.bulk_save_objects(business_data)
        session.bulk_save_objects(location_data)
        session.bulk_save_objects(neighborhood_data)
        session.commit()
        print("Data inserted successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error inserting data: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    engine = create_database()
    insert_data(engine)


#Example of Use:
# Create and use the database
#engine = create_database()

# Insert example data into the tables
#insert_data(engine)
