from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import requests
from data_ingestion import JSONDataLoader, DataLoaderManager
from data_transformation import RestaurantDataCleaner
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

def ingest_json_data():
    json_loader = JSONDataLoader('/path/to/your/data.json')
    data_manager = DataLoaderManager(json_loader, None)
    restaurants_df, _ = data_manager.load_all_data()
    return restaurants_df

def transform_restaurant_data(ti):
    restaurants_df = ti.xcom_pull(task_ids='ingest_json_data')
    cleaner = RestaurantDataCleaner(restaurants_df)
    cleaned_restaurants = cleaner.clean()
    return cleaned_restaurants

def load_restaurant_data(ti):
    cleaned_restaurants = ti.xcom_pull(task_ids='transform_restaurant_data')
    db_manager = DatabaseManager()
    db_manager.load_data(cleaned_restaurants, 'restaurants', 'upsert', unique_key='id')

with DAG(
    'weekly_json_data_pipeline',
    default_args=default_args,
    description='Weekly JSON data pipeline',
    schedule_interval='0 1 * * 1',
) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_json_data',
        python_callable=ingest_json_data,
        on_failure_callback=send_slack_alert,
    )

    transform_task = PythonOperator(
        task_id='transform_restaurant_data',
        python_callable=transform_restaurant_data,
        on_failure_callback=send_slack_alert,
    )

    load_task = PythonOperator(
        task_id='load_restaurant_data',
        python_callable=load_restaurant_data,
        on_failure_callback=send_slack_alert,
    )

    ingest_task >> transform_task >> load_task
