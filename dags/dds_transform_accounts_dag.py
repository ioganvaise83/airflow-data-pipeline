from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from jobs.dds.transform_accounts import run

with DAG(
    dag_id="dds_transform_accounts_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dds", "transform"]
) as dag:

    transform_accounts = PythonOperator(
        task_id="transform_accounts",
        python_callable=run
    )
