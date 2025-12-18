from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from jobs.ods.load_accounts import run

with DAG(
    dag_id="ods_load_accounts_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ods", "load"]
) as dag:

    load_accounts_to_ods = PythonOperator(
        task_id="load_accounts_to_ods",
        python_callable=run
    )
