import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from src.get_auction_list import get_list
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
        op_kwargs={'flat_size': [[0,18]]}
    )

    def get_scraping_task(part):
        task = DockerOperator(
            task_id=f'get_auction_details_{part}',
            image='scraper:latest',
            container_name=f'scraping_container_{part}',
            api_version='auto',
            auto_remove=True,
            command=f"python src/get_auction_details.py {part}",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            mount_tmp_dir=False,
            mounts=[Mount(source=f'{os.environ["APT_DIR"]}/scraper',
                        target='/code',
                        type='bind')],
            environment={'MYSQL_PASSWORD': os.environ['MYSQL_PASSWORD'],
                        'PAPUGA_IP': os.environ['PAPUGA_IP']}
        )
        return task
    
    task3a = get_scraping_task(0)

    task3b = get_scraping_task(1)

    task4 = PythonOperator(
        task_id='clean_data',
        python_callable=clean
    )

    task1 >> task2 >> [task3a, task3b] >> task4
