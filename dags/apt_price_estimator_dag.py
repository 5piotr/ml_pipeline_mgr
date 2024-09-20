import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from src.get_auction_list import get_list
from src.get_auction_details import get_details
from src.clean_data import clean
from src.log_run import log
from src.update_models import update
from src.generate_email import email

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
    dag_id='apt_price_estimator',
    description='get and transform data, train models',
    start_date=datetime(2023, 12, 16),
    schedule='0 3 16 * *',
    catchup=False
) as dag:
    
    task1 = EmailOperator(
        task_id='send_start_email',
        to=os.environ['PIOTR_EMAIL'],
        subject='Airflow Alert',
        html_content='apt_price_estimator started running'
    )

    task2 = PythonOperator(
        task_id='get_auction_list',
        retries=1,
        python_callable=get_list
    )

    task3 = PythonOperator(
        task_id='get_auction_details',
        python_callable=get_details
    )

    task4 = PythonOperator(
        task_id='clean_data',
        python_callable=clean
    )

    task5 = DockerOperator(
        task_id='clustering',
        image='trainer:latest',
        container_name='cluster_container',
        api_version='auto',
        auto_remove=True,
        command="python src/clustering.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=f'{os.environ["APT_DIR"]}/trainer',
                      target='/code',
                      type='bind'),
                Mount(source=f'{os.environ["APT_DIR"]}/models',
                      target='/models',
                      type='bind')],
        environment={'MYSQL_PASSWORD': os.environ['MYSQL_PASSWORD'],
                     'PAPUGA_IP': os.environ['PAPUGA_IP']}
    )

    task6 = DockerOperator(
        task_id='preparing_train_pred_data',
        image='trainer:latest',
        container_name='prepare_container',
        api_version='auto',
        auto_remove=True,
        command="python src/preparing_train_pred_data.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=f'{os.environ["APT_DIR"]}/trainer',
                      target='/code',
                      type='bind'),
                Mount(source=f'{os.environ["APT_DIR"]}/models',
                      target='/models',
                      type='bind')],
        environment={'MYSQL_PASSWORD': os.environ['MYSQL_PASSWORD'],
                     'PAPUGA_IP': os.environ['PAPUGA_IP'],
                     'APT_DIR': os.environ['APT_DIR']}
    )

    task7 = DockerOperator(
        task_id='training_ann',
        image='trainer:latest',
        container_name='ann_container',
        api_version='auto',
        auto_remove=True,
        command="python src/training_ann.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=f'{os.environ["APT_DIR"]}/trainer',
                      target='/code',
                      type='bind'),
                Mount(source=f'{os.environ["APT_DIR"]}/models',
                      target='/models',
                      type='bind')]
    )

    task8 = DockerOperator(
        task_id='training_xgb',
        image='trainer:latest',
        container_name='xgb_container',
        api_version='auto',
        auto_remove=True,
        command="python src/training_xgb.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[Mount(source=f'{os.environ["APT_DIR"]}/trainer',
                      target='/code',
                      type='bind'),
                Mount(source=f'{os.environ["APT_DIR"]}/models',
                      target='/models',
                      type='bind')]
    )

    task9 = PythonOperator(
        task_id='log_run',
        python_callable=log
    )

    task10 = PythonOperator(
        task_id='update_models',
        python_callable=update
    )

    task11 = EmailOperator(
        task_id='send_result_email',
        to=os.environ['PIOTR_EMAIL'],
        subject='Airflow Alert',
        html_content=email()
    )

    task1 >> task2 >> task3 >> task4 >> \
    task5 >> task6 >> task7 >> task8 >> \
    task9 >> task10 >> task11
