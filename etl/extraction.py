import requests
import json
import pandas as pd 
from datetime import datetime 
import os
import sys 
from importlib import reload


dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))

sys.path.append(dir_path)

import connection


def extract_transform_load():
  
    # # Reload connection module
    # connection = reload(connection)

    # Fetch data from API
    response = requests.get(connection.url)         
    api_data = response.text

    if response.status_code == 200:  
        api_data = f''' {api_data} '''
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
        df = df[['Primary_key', 'Currency', 'Rate', 'Base', 'Date', 'Timestamp' ]]
        print("API Data Successfully Extracted")
    else:
        ("Failed to retrieve data. Status code:", response.status_code)


    # Save data to CSV
    csv_file_path = f'../data/exchange_rate_{rate_date}.csv' 
    df.to_csv(csv_file_path, index=False)

    # Compress CSV to gzip
    file_name = csv_file_path + '.gz'
    df.to_csv(file_name, index=False, compression='gzip')

    # Upload to S3
    # compressed_csv_s3 = file_name[2:]
    # with open(file_name, 'rb') as file:
    #     s3_bucket = connection.connect_to_s3()
    #     s3_bucket.upload_fileobj(file, 'emil-coinbase-bucket', compressed_csv_s3)
    # if s3_bucket.head_object(Bucket='emil-coinbase-bucket', Key=compressed_csv_s3):
    #     print("CSV file uploaded to S3 bucket successfully")
    # else:
    #     print("Upload not successful")

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
                            timestamp VARCHAR(16)
                            
                        )'''
        cur_pg.execute(rates_table)
        conn_pg.commit()
        print("Table Creation Successful")
    except Exception as e:
        conn_pg.rollback()
        print("Error occurred:", e)

    # Insert data into local PostgreSQL
    try:
        batch_size = 200
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            values = [tuple(row) for row in batch_df.values]
            placeholders = ','.join(['%s'] * len(df.columns))
            insert_query = f'''INSERT INTO {table_name} (primary_key, currency, rate, base, date, timestamp) 
                              VALUES ({placeholders}) 
                              ON CONFLICT (rate, date, primary_key) DO NOTHING'''
            cur_pg.executemany(insert_query, values)
            conn_pg.commit()
            print('Data Successfully Inserted into Local Postgres DB')
    
    except Exception as e:
        print("An error occurred during batch data insertion:", e)
        conn_pg.rollback()

if __name__ == "__main__":
    extract_transform_load()