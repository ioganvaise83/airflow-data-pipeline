from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from utils.job_loader import load_job

JOB_PATH = "/opt/airflow/jobs/example_job.py"


with DAG(
    dag_id="example_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Example ETL DAG",
) as dag:

    def run_etl():
        job = load_job(JOB_PATH)
        job.run()

    run_task = PythonOperator(
        task_id="run_python_etl",
        python_callable=run_etl,
    )
