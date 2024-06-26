# ETL Pipeline README

This repository contains an ETL (Extract, Transform, Load) pipeline designed to retrieve data from the OpenExchangeRates API, process it, and load it into both a Redshift database and a local PostgreSQL database. Below is an overview of the files and their functionalities:

## Files:

### 1. `config/connection.py`

This file contains the connection parameters for various databases and services used in the ETL pipeline, including Redshift, S3, and local PostgreSQL. Change the parameters to what applies in your local and cloud environment. The Cloud Provider used is Amazon AWS.

### 2. `etl/extraction.py`

This Python script orchestrates the ETL process. It connects to both the Redshift and local PostgreSQL databases, retrieves data from the openexchangerates API, processes it, and uploads it to the databases. The locally persisted files are in the datasets directory.
The files persisted in S3 bucket are in {the-name-of-your-bucket}/data

### 3. `tests directory`

This directory contains unit tests which mocks the connection and extraction

### 4. `README.md`

This README file provides an overview of the ETL pipeline, its components, and how to set up and run the pipeline.

## Setup:

To set up the ETL pipeline, follow these steps:

1. Ensure you have Python installed on your system.

2. Install the required dependencies by running:

   ```bash
   [pip install -r requirements.txt]
3. Test your connection parameters, run the connection.py file inside the config directory 

4. To orchestrate the ETL process, run main.py in the pipeline directory 

5. To use Airflow, copy the pipeline_dag.py file in the dags folder to your airflow dag folder and modify the {root_directory} to reflect that of your environment



