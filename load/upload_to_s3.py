import requests
import json
import pandas as pd 
from datetime import datetime 
import os
import sys 
from importlib import reload

# add the config and etl directories to the sys.path
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.append(dir_path)
dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl'))
sys.path.append(dir_path)

import connection #from config directory
import extraction #from etl directory





bucket_name = 'emil-coinbase-bucket'
file_name = extraction.csv_file_path
compressed_csv_s3 = extraction.compressed_csv_s3 #directory to be used in s3 bucket



# upload to s3 function
def upload_to_s3(file_name, bucket_name):
    try:
        
        with open(file_name, 'rb') as file:
            s3 = connection.connect_to_s3() # Connect to S3
            s3.upload_fileobj(file, bucket_name, compressed_csv_s3)  # Upload file to S3

        # Check if the file exists in the S3 bucket
        response = s3.head_object(Bucket=bucket_name, Key=compressed_csv_s3)
        if response:
            print("CSV file uploaded to S3 bucket successfully")
        else:
            print("Upload not successful")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

        
if __name__ == "__main__":
    upload_to_s3()
