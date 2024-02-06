from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def push_data(ti):
    message = "Hello, this is a message from Task A!"
    ti.xcom_push(key='my_message_key', value=message)
    print(f"Pushed data: {message}")

def pull_data(ti):
    pulled_message = ti.xcom_pull(task_ids='push_task', key='my_message_key')
    print(f"Pulled data: {pulled_message}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xcom_example_dag',
    default_args=default_args,
    description='An example DAG using XCom',
    schedule_interval=timedelta(days=1),
)

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_data,
    provide_context=True,
    dag=dag,
)

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_data,
    provide_context=True,
    dag=dag,
)

push_task >> pull_task
