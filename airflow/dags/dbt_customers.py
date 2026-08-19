from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dbt_customers",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:

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

    dbt_run >> dbt_test