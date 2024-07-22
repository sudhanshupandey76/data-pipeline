from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import subprocess
import requests
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='An ETL data pipeline',
    schedule_interval='0 12 * * *',
)

# Function to run a Python script
def run_script(script_path):
    subprocess.run([sys.executable, script_path], check=True)

# Function to send Slack alert
def send_slack_alert(context):
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    task_instance = context['task_instance']
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    log_url = task_instance.log_url

    message = f"""
    :red_circle: Task Failed.
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Time*: {execution_date}
    *Log URL*: {log_url}
    """

    payload = {'text': message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        raise ValueError(f'Request to Slack returned an error {response.status_code}, the response is:\n{response.text}')

# Tasks to run the scripts
ingest_data = PythonOperator(
    task_id='ingest_data',
    python_callable=run_script,
    op_args=['/data_ingestion.py'],
    on_failure_callback=send_slack_alert,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=run_script,
    op_args=['/data_transformation.py'],
    on_failure_callback=send_slack_alert,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=run_script,
    op_args=['/data_loading.py'],
    on_failure_callback=send_slack_alert,
    dag=dag,
)

# Set task dependencies
ingest_data >> transform_data >> load_data
