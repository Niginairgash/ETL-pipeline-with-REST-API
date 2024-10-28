from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# import functions for ETL
from etl_script_rest_api import extract_data, transform_data, load_data_to_db

# Default arguments
default_args = {
    'owner' : 'airflow',
    'start_date': datetime(2024, 10, 8),
    'retries' : 1
}

# URL API
api_url = 'https://api.spacexdata.com/v3/history'

# String connection to db
db_connection_string = 'postgresql://airflow:airflow@new-airflow-postgres-1:5432/spacex_events'

# Create DAG
with DAG('etl_spacex_api_pipeline', default_args=default_args, schedule_interval='@daily') as dag:

    # Tasks ETL
    def extract_task():
        return extract_data(api_url)
    
    def transform_task():
        events = extract_task()
        return transform_data(events)
    
    def load_task():
        df = transform_task()
        load_data_to_db(df, db_connection_string)
    
    # Define tasks in Airflow
    extract = PythonOperator(
        task_id = 'extract_data',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id = 'transform_data',
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id = 'load_data',
        python_callable=load_task
    )

    extract >> transform >> load
