import requests
import json
import pandas as pd 
from datetime import datetime 
import os
import sys 
import csv
from importlib import reload

dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.append(dir_path)

import connection

url = connection.url
print(url)

def extract_transform_load():
  
    # # Reload connection module
    # connection = reload(connection)
    pass
    # Fetch data from API
    response = requests.get(url)         
    api_data = response.text

    if response.status_code == 200:  
        api_data = api_data.replace("\n", "").replace("'", '"')
        api_data
        api_data = json.loads(api_data)
        # api_data
        df = pd.DataFrame(api_data)

        # extract base and timestamp column data
        base = api_data['base']
        timestamp = api_data['timestamp']

        # Get date and time from timestamp 
        date = datetime.fromtimestamp(timestamp)
        # format date to YYYYMMDD to be used as a column in the dataframe
        year = date.strftime('%Y-%m-%d')
        # format time to HMS
        time = date.strftime('%H%M%S')
        # this will be part of the csv filename to ensure uniqueness of filename 
        rate_date = year + '_'+ time
    
        df = pd.DataFrame(api_data['rates'].items(), columns=['Currency', 'Rate'])

        # format year without '-' hyphens, and hour to be used as part of primary key
        primary_key_year = date.strftime('%Y%m%d') + time[:2]
        # Add base, timestamp, and date columns
        df['Primary_key'] = df['Currency'] + primary_key_year
        df['Base'] = base
        df['Timestamp'] = timestamp 
        df['Date'] = year
        # Create a rate_checker column to check for uchanged daily rates 
        df['Rate_Checker'] = df['Currency'] + primary_key_year + df['Rate'].astype(str).str.replace(".", "").str[:5]

        df = df[['Primary_key', 'Currency', 'Rate', 'Base', 'Date', 'Timestamp', 'Rate_Checker' ]]
        print("API Data Successfully Extracted")
    else:
        ("Failed to retrieve data. Status code:", response.status_code)

    data_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    
    # Save data to CSV
    csv_file_path = f'{data_directory}/exchange_rate_{rate_date}.csv' 
    df.to_csv(csv_file_path, index=False)
   
    # Compress CSV to gzip to upload to S3 
    file_name = csv_file_path + '.gz'
    df.to_csv(file_name, index=False, compression='gzip')

    compressed_csv_s3 = file_name[2:] #this slicing of the file path will takeout the leading ../ 
    #so as to make the directory in s3 properly organized

    # Upload to S3
    compressed_csv_s3 = file_name[2:]
    with open(file_name, 'rb') as file:
        s3_bucket = connection.connect_to_s3()
        s3_bucket.upload_fileobj(file, 'data-engineer', compressed_csv_s3)
    if s3_bucket.head_object(Bucket='data-engineer', Key=compressed_csv_s3):
        print("CSV file uploaded to S3 bucket successfully")
    else:
        print("Upload not successful")

    # Create table in local PostgreSQL
    table_name = 'exchange_rates'
    conn_pg = connection.connect_to_local_postgres()
    cur_pg = conn_pg.cursor()
    try:
        rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                            primary_key varchar PRIMARY KEY, 
                            currency VARCHAR(3),
                            rate NUMERIC(16, 8),
                            base VARCHAR(3),
                            date Date,
                            timestamp VARCHAR(16),
                            rate_checker VARCHAR(45) UNIQUE
                            
                        )'''
        cur_pg.execute(rates_table)
        conn_pg.commit()
        print("Table Creation Successful")
    except Exception as e:
        conn_pg.rollback()
        print("Error occurred:", e)

    # Insert data into local PostgreSQL

    # Open the CSV file
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row if it exists
        for row in reader:
            # Insert each row into the PostgreSQL table
            cur_pg.execute(
            '''INSERT INTO exchange_rates_demo (primary_key, currency, rate, base, date, timestamp, rate_checker) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s )
                                ON CONFLICT(rate_checker) 
                                DO UPDATE
                                SET primary_key = EXCLUDED.primary_key,
                                currency = EXCLUDED.currency,
                                rate = EXCLUDED.rate,
                                base = EXCLUDED.base,
                                date = EXCLUDED.date,
                                timestamp = EXCLUDED.timestamp; ''',
                row
            )

    # Commit the transaction
    conn_pg.commit()

    # Close the cursor and connection
    cur_pg.close()
    conn_pg.close()



# REDSHIFT TABLE CREATION AND FILE COPY FROM S3 BUCKET

# %%
    # create table REDSHIFT
    print("Create Table Redshift")
    table_name = 'exchange_rate_tbl'
    try:
        
        rates_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (
                                    primary_key varchar PRIMARY KEY, 
                                    currency VARCHAR(3),
                                    rate NUMERIC(16, 8),
                                    base VARCHAR(3),
                                    date Date,
                                    timestamp VARCHAR(16),
                                    rate_checker VARCHAR(45) UNIQUE
                                    
                            
                            )'''
            
        conn_redshift = connection.connect_to_redshift() 
        cur_redshift = conn_redshift.cursor()
        conn_redshift.commit()
        print(f"Redshift Table {table_name} created successfully.")

    except Exception as e:
        connection.conn_redshift.rollback()
        print(f"Error creating table: {e}")

    # %%
    # upload data to Redshift form s3 bucket


    aws_access_key_id = connection.aws_access_key_id
    aws_secret_access_key = connection.aws_secret_access_key


    try:
        insert_from_s3 = f"""COPY {table_name}
        FROM 's3://open-exchange-rates/{compressed_csv_s3}'
        CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
        IGNOREHEADER 1
        FORMAT AS csv
        gzip
        DELIMITER ','; """
        
        connection.cur_redshift.execute(insert_from_s3)
        connection.conn_redshift.commit()
        print(f"Copy to Redshift: {table_name} was successful")

    except Exception as e:
        connection.conn_redshift.rollback()
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the Redshift connection
        connection.conn_redshift.close()
