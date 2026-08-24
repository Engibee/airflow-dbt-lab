from datetime import datetime, timedelta

#THIS IS A TEST

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def load_customers():
    hook = PostgresHook(
        postgres_conn_id="analytics_postgres"
    )

    conn = hook.get_conn()

    try:
        cursor = conn.cursor()

        # 1. Busca o watermark atual
        cursor.execute("""
            SELECT last_updated_at
            FROM raw.ingestion_control
            WHERE pipeline_name = 'customers'
        """)

        result = cursor.fetchone()
        last_updated_at = result[0]

        # 2. Busca somente os registros novos/alterados
        cursor.execute("""
            SELECT id, name, email, updated_at
            FROM external.customers
            WHERE updated_at > %s
            ORDER BY updated_at, id
        """, (last_updated_at,))

        customers = cursor.fetchall()

        print(f"Watermark atual: {last_updated_at}")
        print(f"Novos registros encontrados: {customers}")

        if not customers:
            print("Nenhum registro novo encontrado.")
            conn.commit()
            return

        # 3. Faz o upsert no RAW
        for customer in customers:
            cursor.execute("""
                INSERT INTO raw.raw_customers
                    (id, name, email, updated_at)
                VALUES
                    (%s, %s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    updated_at = EXCLUDED.updated_at
            """, customer)

        # 4. Atualiza o watermark
        new_watermark = customers[-1][3]

        cursor.execute("""
            UPDATE raw.ingestion_control
            SET last_updated_at = %s
            WHERE pipeline_name = 'customers'
        """, (new_watermark,))

        # 5. Confirma TUDO
        conn.commit()

        print(f"Novo watermark: {new_watermark}")

    except Exception:
        # Se qualquer etapa falhar, desfaz tudo
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

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