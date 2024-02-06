from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2023,12,28)
}

def _downloading():
    print('downloading')

with DAG('trigger_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False) as dag:

    _downloading = PythonOperator(
        task_id='downloading',
        python_callable=_downloading
    )

    # Creating a task
    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target',
        trigger_dag_id='target_dag'
    )