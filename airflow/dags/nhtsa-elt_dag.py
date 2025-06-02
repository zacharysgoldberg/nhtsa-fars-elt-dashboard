from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'zachary',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_dag():
    os.system('python /opt/airflow/elt/main.py')


with DAG(
    dag_id='nhtsa_elt_pipeline',
    default_args=default_args,
    description='Run NHTSA ELT and dbt models',
    start_date=datetime(2025, 5, 30),
    schedule_interval='@semi_annually',  # or '0 0 1 1,7 *'
    catchup=False,
    tags=['nhtsa'],
) as dag:

    run_elt_script = BashOperator(
        task_id='run_elt_script',
        bash_command='python /opt/airflow/elt/main.py'
    )

    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/elt/fars_dbt && dbt run'
    )

    run_elt_script >> run_dbt
