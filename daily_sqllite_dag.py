from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import requests
from data_ingestion import SQLiteDataLoader, DataLoaderManager
from data_transformation import ReviewDataCleaner
from data_loading import DatabaseManager

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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

def ingest_sqlite_data():
    sqlite_loader = SQLiteDataLoader('/path/to/your/database.sqlite', 'SELECT * FROM reviews')
    data_manager = DataLoaderManager(None, sqlite_loader)
    _, reviews_df = data_manager.load_all_data()
    return reviews_df

def transform_review_data(ti):
    reviews_df = ti.xcom_pull(task_ids='ingest_sqlite_data')
    cleaner = ReviewDataCleaner(reviews_df)
    cleaned_reviews = cleaner.clean()
    return cleaned_reviews

def load_review_data(ti):
    cleaned_reviews = ti.xcom_pull(task_ids='transform_review_data')
    db_manager = DatabaseManager()
    db_manager.load_data(cleaned_reviews, 'reviews', 'upsert', unique_key='Id')

with DAG(
    'daily_sqlite_data_pipeline',
    default_args=default_args,
    description='Daily SQLite data pipeline',
    schedule_interval='0 12 * * *',
) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_sqlite_data',
        python_callable=ingest_sqlite_data,
        on_failure_callback=send_slack_alert,
    )

    transform_task = PythonOperator(
        task_id='transform_review_data',
        python_callable=transform_review_data,
        on_failure_callback=send_slack_alert,
    )

    load_task = PythonOperator(
        task_id='load_review_data',
        python_callable=load_review_data,
        on_failure_callback=send_slack_alert,
    )

    ingest_task >> transform_task >> load_task
