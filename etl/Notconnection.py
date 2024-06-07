import psycopg2
import boto3
import os
import sys
from pathlib import Path
from dotenv import load_dotenv, dotenv_values 

BASE_DIR = Path(".")
ENV_DIR = BASE_DIR / "env"
DOT_ENV = ENV_DIR / ".env"

load_dotenv(DOT_ENV, override=True)

# API URL
url = os.getenv('url')
print(url)

# Postgres Connection Parameters
pg_dbname = os.getenv("POSTGRES_DATABASE")
pg_host = os.getenv("POSTGRES_HOST")
pg_port = os.getenv("POSTGRES_PORT")
pg_user= os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")

# S3 Connection Parameters
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name= os.getenv('REGION_NAME')

# Redshift Conection Parameters
redshift_dbname = os.getenv('REDSHIFT_DBNAME')
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_user = os.getenv('REDSHIFT_USER ')
redshift_password = os.getenv('REDSHIFT_PASSWORD ')

# dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
# sys.path.append(dir_path)
# import config
# from config import redshift_dbname, redshift_host, redshift_port, redshift_user, redshift_password, aws_access_key_id, aws_secret_access_key, region_name, pg_dbname, pg_host, pg_port, pg_user, pg_password, url



def connect_to_redshift():
    pass
    try:
        conn_redshift = psycopg2.connect(
            dbname=redshift_dbname,
            host=redshift_host,
            port=redshift_port,
            user=redshift_user,
            password=redshift_password
        )
        return conn_redshift
    except Exception as e:
        print("Error connecting to Redshift:", e)
        return None

def connect_to_s3():
    pass
    try:
        s3_bucket = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        return s3_bucket
    except Exception as e:
        print("Error connecting to S3:", e)
        return None

def connect_to_local_postgres():
    try:
        conn_pg = psycopg2.connect(
            dbname=pg_dbname,
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password
        )
        return conn_pg
    except Exception as e:
        print("Error connecting to local PostgreSQL:", e)
        return None
