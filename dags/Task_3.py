from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


# Define the DAG
with DAG(
    dag_id="my_15_minute_dag",
    # start_date=days_ago(1),
    schedule_interval=timedelta(minutes=15),
) as dag:

    def my_15_minute_task():
        print("This DAG has been running for every 15 minutes!")
        # Add your specific task logic here

    # Create the PythonOperator
    run_every_15_minutes = PythonOperator(
        task_id="run_every_15_minutes",
        python_callable=my_15_minute_task,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
