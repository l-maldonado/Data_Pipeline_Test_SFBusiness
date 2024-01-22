# test_data_cleaner.py
import unittest
from data_cleaner import clean_transform_data

class TestDataCleaner(unittest.TestCase):
    def test_clean_transform_data_success(self):
        data = {'mock_data': 'some_value'}
        result = clean_transform_data(data)
        self.assertIsNotNone(result)

    def test_clean_transform_data_failure(self):
        data = None
        result = clean_transform_data(data)
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()