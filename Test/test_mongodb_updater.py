# test_mongodb_updater.py
import unittest
from mongodb_updater import upload_to_mongodb
import pandas as pd

class TestMongoDBUpdater(unittest.TestCase):
    def test_upload_to_mongodb_success(self):
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        mongodb_uri = 'mongodb://localhost:27017/'
        db_name = 'test_database'
        collection_name = 'test_collection'
        upload_to_mongodb(df, mongodb_uri, db_name, collection_name)
        # You can add assertions to check if data is uploaded successfully

    def test_upload_to_mongodb_failure(self):
        df = None
        mongodb_uri = 'mongodb://localhost:27017/'
        db_name = 'test_database'
        collection_name = 'test_collection'
        upload_to_mongodb(df, mongodb_uri, db_name, collection_name)
        # You can add assertions to check if the failure is handled appropriately

if __name__ == '__main__':
    unittest.main()