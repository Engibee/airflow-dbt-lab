import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_airflow():
    logging.info("Hello from Airflow pipeline!")


with DAG(
    dag_id="hello_pipeline",
    description="My first Airflow pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
    },
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_airflow,
    )