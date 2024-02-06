from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="my_simple_dag",
    schedule_interval=None,
) as dag:
    
    start_task = DummyOperator(task_id="start")
    step1 = DummyOperator(task_id="step1")
    step2 = DummyOperator(task_id="step2")
    end_task = DummyOperator(task_id="end")

    # tasks
    start_task >> step1 >> step2 >> end_task
    