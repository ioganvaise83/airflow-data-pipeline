from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def transform_ods_to_dds():
    conn = psycopg2.connect(
        host="postgres", 
        database="airflow", 
        user="airflow", 
        password="airflow"
    )
    cursor = conn.cursor()

    # 1. Выбираем сырые данные
    cursor.execute("SELECT account_number, contractor_inn, amount, created_at FROM ods.accounts")
    rows = cursor.fetchall()

    for acc_number, inn, amount, created_at in rows:

        # 2. Добавляем в DDS.contractors, если нет
        cursor.execute("""
            INSERT INTO dds.contractors(inn)
            VALUES (%s)
            ON CONFLICT (inn) DO NOTHING
            RETURNING id
        """, (inn,))

        result = cursor.fetchone()

        if result is None:
            # Уже существует — берём id
            cursor.execute("SELECT id FROM dds.contractors WHERE inn = %s", (inn,))
            contractor_id = cursor.fetchone()[0]
        else:
            contractor_id = result[0]

        # 3. Добавляем нормализованный счет
        cursor.execute("""
            INSERT INTO dds.accounts(account_number, contractor_id, amount, created_at)
            VALUES (%s, %s, %s, %s)
        """, (acc_number, contractor_id, amount, created_at))

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="dds_transform_accounts_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    transform = PythonOperator(
        task_id="normalize_and_load_dds",
        python_callable=transform_ods_to_dds
    )
