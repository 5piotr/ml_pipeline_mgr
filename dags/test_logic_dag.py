import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.get_auction_list import get_list
from src.get_auction_details import get_details
from src.clean_data import clean

default_args = {
    'owner':'piotr',
    'retries':0,
    'retry_delay':timedelta(minutes=3),
    'email': [os.environ['PIOTR_EMAIL']],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    default_args=default_args,
    dag_id='test_logic',
    description='test logic and sending mail',
    start_date=datetime(2024, 1, 1),
    schedule='0 3 2 * *',
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='get_auction_list',
        python_callable=get_list
    )

    task2 = PythonOperator(
        task_id='get_auction_details',
        python_callable=get_details
    )

    task3 = PythonOperator(
        task_id='clean_data',
        python_callable=clean
    )

    task1 >> task2 >> task3
