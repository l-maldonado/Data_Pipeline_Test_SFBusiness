# test_pipeline.py
import unittest
from unittest.mock import patch
from pipeline import data_pipeline

class TestPipeline(unittest.TestCase):
    @patch('pipeline.fetch_data', return_value={'mock_data': 'some_value'})
    @patch('pipeline.clean_transform_data', return_value=pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}))
    @patch('pipeline.upload_to_mongodb')
    def test_data_pipeline_success(self, mock_fetch_data, mock_clean_transform_data, mock_upload_to_mongodb):
        api_url = 'your_mock_api_url'
        mongodb_uri = 'mongodb://localhost:27017/'
        db_name = 'test_database'
        collection_name = 'test_collection'
        data_pipeline(api_url, mongodb_uri, db_name, collection_name)
        # You can add assertions to check if pipeline steps are executed successfully

    @patch('pipeline.fetch_data', return_value=None)
    @patch('pipeline.clean_transform_data', return_value=None)
    @patch('pipeline.upload_to_mongodb')
    def test_data_pipeline_failure(self, mock_fetch_data, mock_clean_transform_data, mock_upload_to_mongodb):
        api_url = 'nonexistent_api_url'
        mongodb_uri = 'mongodb://localhost:27017/'
        db_name = 'test_database'
        collection_name = 'test_collection'
        data_pipeline(api_url, mongodb_uri, db_name, collection_name)
        # You can add assertions to check if failure is handled appropriately

if __name__ == '__main__':
    unittest.main()