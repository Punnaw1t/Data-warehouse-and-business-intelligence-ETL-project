"""
Apache Airflow DAG for Major Cineplex ETL Pipeline
Orchestrates the extraction, transformation, and loading of transaction data
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging
import os
import sys

# Add parent directory to path to import ETL jobs
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dags.etl_jobs.EtlJobForMajor import EtlJobForMajor

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
dag = DAG(
    'major_cineplex_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Major Cineplex transaction data processing',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['etl', 'cineplex', 'transactions'],
)


def run_etl_job(**context):
    """
    Execute the ETL job to process transaction data
    """
    try:
        # Get configuration from Airflow variables or environment
        source_file = os.getenv('ETL_SOURCE_FILE', '/opt/data/transaction.csv')
        output_path = os.getenv('ETL_OUTPUT_PATH', '/opt/data/output')
        
        logger.info(f"Starting ETL job with source: {source_file}")
        
        # Initialize and run the ETL job
        etl_job = EtlJobForMajor(
            source=source_file,
            destination=output_path
        )
        
        result = etl_job.run()
        
        logger.info(f"ETL job completed successfully. Output: {output_path}")
        
        # Return the output path for downstream tasks
        return {
            'status': 'success',
            'output_path': output_path,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise


def validate_output(**context):
    """
    Validate that the output file was created successfully
    """
    task_instance = context['task_instance']
    etl_result = task_instance.xcom_pull(task_ids='run_etl')
    
    output_path = etl_result['output_path']
    
    # Check if output directory/file exists
    if os.path.exists(output_path):
        logger.info(f"Output validation successful. Files found at: {output_path}")
        # Count CSV files in output
        csv_count = len([f for f in os.listdir(output_path) if f.endswith('.csv')])
        logger.info(f"Generated {csv_count} CSV files")
        return True
    else:
        raise Exception(f"Output validation failed. Path does not exist: {output_path}")


# Define tasks
task_extract_and_transform = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl_job,
    provide_context=True,
    dag=dag,
)

task_validate_output = PythonOperator(
    task_id='validate_output',
    python_callable=validate_output,
    provide_context=True,
    dag=dag,
)

task_log_completion = BashOperator(
    task_id='log_completion',
    bash_command='echo "ETL pipeline completed at $(date)" && echo "Output path: $ETL_OUTPUT_PATH"',
    env={'ETL_OUTPUT_PATH': os.getenv('ETL_OUTPUT_PATH', '/opt/data/output')},
    dag=dag,
)

# Define task dependencies
task_extract_and_transform >> task_validate_output >> task_log_completion
