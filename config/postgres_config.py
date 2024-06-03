import os 
from dotenv import load_dotenv, dotenv_values 

load_dotenv()

POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


