from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
"owner": "airflow",
}

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="""
        cd /opt/airflow/dbt &&
        dbt seed --profiles-dir .
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt &&
        dbt run --profiles-dir .
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt &&
        dbt test --profiles-dir .
        """,
    )

    dbt_seed >> dbt_run >> dbt_test
