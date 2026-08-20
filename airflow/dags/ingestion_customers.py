from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def load_customers():
    hook = PostgresHook(
        postgres_conn_id="analytics_postgres"
    )

    watermark = hook.get_first("""
        SELECT last_updated_at
        FROM raw.ingestion_control
        WHERE pipeline_name = 'customers'
    """)

    last_updated_at = watermark[0]

    customers = hook.get_records("""
    SELECT id, name, email, updated_at
    FROM external.customers
    WHERE updated_at > %s
    ORDER BY updated_at, id
""", parameters=(last_updated_at,))

    print(f"Watermark: {last_updated_at}")
    print(f"Novos registros encontrados: {customers}")

    if customers:
        hook.insert_rows(
        table="raw.raw_customers",
        rows=customers,
        target_fields=["id", "name", "email", "updated_at"],
        replace=True,
        replace_index=["id"],
    )

    new_watermark = customers[-1][3]

    hook.run(
        """
        UPDATE raw.ingestion_control
        SET last_updated_at = %s
        WHERE pipeline_name = 'customers'
        """,
        parameters=(new_watermark,),
    )


with DAG(
    dag_id="ingestion_customers",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),

    },
) as dag:

    load_customers_task = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_customers",
        trigger_dag_id="dbt_customers",
        wait_for_completion=True,
    )

    load_customers_task >> trigger_dbt