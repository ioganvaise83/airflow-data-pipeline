from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def load_to_ods():
    conn = psycopg2.connect(
        host="postgres", 
        database="airflow", 
        user="airflow", 
        password="airflow"
    )

    cursor = conn.cursor()

    sample_data = [
        ("ACC-001", "7701234567", 1000),
        ("ACC-002", "7809876543", 2500),
        ("ACC-003", "7701234567", 500),
    ]

    for acc in sample_data:
        cursor.execute("""
            INSERT INTO ods.accounts(account_number, contractor_inn, amount)
            VALUES (%s, %s, %s)
        """, acc)

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="ods_load_accounts_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    load_accounts = PythonOperator(
        task_id="load_accounts_to_ods",
        python_callable=load_to_ods
    )
