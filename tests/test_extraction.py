import unittest
from unittest.mock import MagicMock, patch
from etl.extraction import extract_transform_load

class TestDataExtraction(unittest.TestCase):

    @patch('etl.extraction.requests.get')
    @patch('etl.extraction.connection.connect_to_s3')
    @patch('etl.extraction.connection.connect_to_local_postgres')
    def test_extract_transform_load_success(self, mock_connect_to_local_postgres, mock_connect_to_s3, mock_requests_get):
        # Mock the response from the API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"rates": {"USD": 1.23, "EUR": 0.99}, "base": "USD", "timestamp": 1622659200}'
        mock_requests_get.return_value = mock_response

        # Mock the S3 bucket and local PostgreSQL connection
        mock_s3_bucket = MagicMock()
        mock_conn_pg = MagicMock()
        mock_connect_to_s3.return_value = mock_s3_bucket
        mock_connect_to_local_postgres.return_value = mock_conn_pg

        # Call the function under test
        extract_transform_load()

        # Assertions
        mock_requests_get.assert_called_once()  # Ensure requests.get() is called
        mock_s3_bucket.upload_fileobj.assert_called_once()  # Ensure S3 upload is called
        mock_conn_pg.execute.assert_called()  # Ensure SQL execution is called

    @patch('etl.extraction.requests.get')
    def test_extract_transform_load_failure(self, mock_requests_get):
        # Mock the response from the API with a failed status code
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_requests_get.return_value = mock_response

        # Call the function under test
        extract_transform_load()

        # Assertions
        mock_requests_get.assert_called_once()  # Ensure requests.get() is called with correct parameters

if __name__ == '__main__':
    unittest.main()
