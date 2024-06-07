from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# COPY THIS DAG FILE TO THE AIRFLOW DAG DIRECTORY AND 
# TARGET THIS REPO FROM THE DAG DIRECTORY, BY REPLACING THE VALUE OF THE VARIABLE 'root_location'


start_date = days_ago(1)
schedule_interval = '0 8,18 * * *'  # Runs at 8:00 and 18:00 UTC every day
root_location = '$HOME/Desktop/Trestle/Git/capstone_exchange_rates' #location of scripts which will be run by airflow dag


# Define the default arguments for the DAG
default_args = {
    'owner': 'ben',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

# Define the DAG with the specified schedule interval
with DAG(
    dag_id='elt_open_exchange_rate',
    default_args=default_args,
    description='Exchange Rates ELT, daily run',
    start_date=start_date,
    schedule_interval=schedule_interval,
    catchup=True
) as dag:
    
    extract_rates = BashOperator(
        task_id='extract_exchange_rate',
        bash_command=f"python3 {root_location}etl/extraction.py",
        dag=dag
    )
    # upload_to_s3 = BashOperator(
    #     task_id = 'upload_exchange_rate_CSV_to_s3',
    #     bash_command=f"python3 {root_location}load/upload_to_s3.py",
    #     dag=dag,
    # )
    # copy_to_redshift = BashOperator(
    #     task_id = 'copy_to_redshift',
    #     bash_command=f"python3 {root_location}load/copy_to_redshift.py",
    #     dag=dag
    # )

extract_rates#>>upload_to_s3 #>>copy_to_redshift


