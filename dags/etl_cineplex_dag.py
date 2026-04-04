from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# Import EtlJobForMajor from etl_jobs
from etl_jobs.EtlJobForMajor import EtlJobForMajor

def run_major_etl():
    # Create Path to make it same Mount in Docker
    input_p = '/opt/airflow/data/transaction.csv'
    output_p = '/opt/airflow/data/output'
    
    job = EtlJobForMajor(source=input_p, destination=output_p)
    job.run()

with DAG(
    'major_cineplex_etl_pipeline',
    default_args={'owner': 'airflow'},
    start_date=days_ago(1),
    schedule_interval=None, # (Manual Trigger)
    catchup=False
) as dag:

    task_etl = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_major_etl
    )