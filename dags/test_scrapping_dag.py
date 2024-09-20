import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

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
    dag_id='test_scrapping',
    description='get and transform data, train models',
    start_date=datetime(2023, 12, 16),
    schedule=None,
    catchup=False
) as dag:
    
    task1 = EmailOperator(
        task_id='send_start_email',
        to=os.environ['PIOTR_EMAIL'],
        subject='Airflow Alert',
        html_content='apt_price_estimator TEST started running'
    )

    task2 = PythonOperator(
        task_id='get_auction_list',
        retries=1,
        python_callable=get_list,
        # op_kwargs={'flat_size': [[0,20]]}
    )

    task3 = PythonOperator(
        task_id='get_auction_details',
        python_callable=get_details
    )

    task4 = PythonOperator(
        task_id='clean_data',
        python_callable=clean
    )

    task1 >> task2 >> task3 >> task4
