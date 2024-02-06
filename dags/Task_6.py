from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    'start_date': datetime(2024, 1, 4),
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="shortcircuit_demo",
    default_args=default_args,
    schedule_interval=None,  
) as dag:

    # Define a function to check a simple condition
    def check_data_availability():
        # Simulate data availability check
        data_available = True 
        return data_available

    # ShortCircuitOperator based on the check function
    check_data = ShortCircuitOperator(
        task_id="check_data_availability",
        python_callable=check_data_availability,
    )

    # Tasks to be skipped if the condition is not met
    def process_data():
        print("Processing data...")

    def send_report():
        print("Sending report...")

    task1 = PythonOperator(task_id="process_data", python_callable=process_data)
    task2 = PythonOperator(task_id="send_report", python_callable=send_report)

    # Connect tasks with the ShortCircuitOperator
    check_data >> [task1, task2]
