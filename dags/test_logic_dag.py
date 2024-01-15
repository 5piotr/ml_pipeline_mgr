import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.log_run import log
from src.update_models import update

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
        task_id='log_run',
        python_callable=log
    )

    task2 = PythonOperator(
        task_id='update_models',
        python_callable=update
    )

    task1 >> task2
