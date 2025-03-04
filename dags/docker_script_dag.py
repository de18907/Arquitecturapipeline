from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ejecutar_script_docker',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    run_script = DockerOperator(
        task_id='run_script',
        image='imagen_api_yfinance:latest',
        api_version='auto',
        auto_remove=True,
        command="python extract_load.py",  # Comando que se ejecuta dentro del contenedor
        docker_url="unix://var/run/docker.sock",  # Permite a Airflow comunicarse con Docker
        network_mode="bridge"
    )

    run_script
