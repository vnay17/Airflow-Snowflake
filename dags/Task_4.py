from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2023, 12, 28)
}

# def _cleaning():
#     print('Cleaning from target DAG')

with DAG('target_dag',
    schedule_interval='@daily',
    default_args=default_args) as dag:

    storing = BashOperator(
        task_id='storing',
        bash_command='sleep 1'
    )