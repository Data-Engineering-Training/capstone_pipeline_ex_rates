import unittest
from unittest.mock import MagicMock, patch
import os
import sys 
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pipeline'))


sys.path.append(dir_path)

import connection
# from ..connection import *
# import connection

class TestConnection(unittest.TestCase):

    @patch('connection.psycopg2.connect')
    def test_connect_to_redshift_success(self, mock_psycopg2_connect):
        # Mock psycopg2.connect to return a mock connection object
        mock_connection = MagicMock()
        mock_psycopg2_connect.return_value = mock_connection

        # Call the function under test
        result = connection.connect_to_redshift()

        # Assertions
        mock_psycopg2_connect.assert_called_once_with(
            dbname=connection.redshift_dbname,
            host=connection.redshift_host,
            port=connection.redshift_port,
            user=connection.redshift_user,
            password=connection.redshift_password
        )
        self.assertEqual(result, mock_connection)

    @patch('connection.boto3.client')
    def test_connect_to_s3_success(self, mock_boto3_client):
        # Mock boto3.client to return a mock S3 bucket object
        mock_s3_bucket = MagicMock()
        mock_boto3_client.return_value = mock_s3_bucket

        # Call the function under test
        result = connection.connect_to_s3()

        # Assertions
        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id=connection.aws_access_key_id,
            aws_secret_access_key=connection.aws_secret_access_key,
            region_name=connection.region_name
        )
        self.assertEqual(result, mock_s3_bucket)

    @patch('connection.psycopg2.connect')
    def test_connect_to_local_postgres_success(self, mock_psycopg2_connect):
        # Mock psycopg2.connect to return a mock connection object
        mock_connection = MagicMock()
        mock_psycopg2_connect.return_value = mock_connection

        # Call the function under test
        result = connection.connect_to_local_postgres()

        # Assertions
        mock_psycopg2_connect.assert_called_once_with(
            dbname=connection.pg_dbname,
            host=connection.pg_host,
            port=connection.pg_port,
            user=connection.pg_user,
            password=connection.pg_password
        )
        self.assertEqual(result, mock_connection)

if __name__ == '__main__':
    unittest.main()
