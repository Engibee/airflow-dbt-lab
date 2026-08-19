from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def load_customers():
    hook = PostgresHook(
        postgres_conn_id="analytics_postgres"
    )

    # 1. Lê o último timestamp processado
    watermark = hook.get_first(
        """
        SELECT last_updated_at
        FROM raw.ingestion_control
        WHERE pipeline_name = 'customers'
        """
    )[0]

    print(f"Watermark atual: {watermark}")

    # 2. Extrai da fonte externa
    customers = hook.get_records(
        """
        SELECT id, name, email, updated_at
        FROM external.customers
        WHERE updated_at > %s
        ORDER BY updated_at
        """,
        parameters=(watermark,),
    )

    if not customers:
        print("Nenhum registro novo ou alterado.")
        return

    print(f"Encontrados {len(customers)} registros.")

    # 3. Carrega na RAW
    hook.insert_rows(
        table="raw.raw_customers",
        rows=customers,
        target_fields=["id", "name", "email", "updated_at"],
        replace=True,
        replace_index=["id"],
    )

    # 4. Avança o watermark
    new_watermark = max(row[3] for row in customers)

    hook.run(
        """
        UPDATE raw.ingestion_control
        SET last_updated_at = %s
        WHERE pipeline_name = 'customers'
        """,
        parameters=(new_watermark,),
    )

    print(f"Watermark atualizado para: {new_watermark}")


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