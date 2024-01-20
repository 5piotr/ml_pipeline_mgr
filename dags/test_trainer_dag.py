import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount



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
    dag_id='test_trainer',
    description='testing trainer',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    task1 = DockerOperator(
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

    task2 = DockerOperator(
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

    task3 = DockerOperator(
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

    task4 = DockerOperator(
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

    task1 >> task2 >> task3 >> task4