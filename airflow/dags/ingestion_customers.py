from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_customers():
    customers = [
    (1, "Ana", "ana@email.com"),
    (2, "Joao Silva", "joao@email.com"),
    (3, "Maria", "maria@email.com"),
    (5, "Pedro", "pedro@email.com"),
]

    hook = PostgresHook(
        postgres_conn_id="analytics_postgres"
    )

    hook.insert_rows(
        table="raw.raw_customers",
        rows=customers,
        target_fields=["id", "name", "email"],
        replace=True,
        replace_index=["id"],
    )


with DAG(
    dag_id="ingestion_customers",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    load_customers_task = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt &&
        DBT_LOG_PATH=/tmp/dbt-logs dbt run --profiles-dir .
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt &&
        DBT_LOG_PATH=/tmp/dbt-logs dbt test --profiles-dir .
        """,
    )

    load_customers_task >> dbt_run >> dbt_test