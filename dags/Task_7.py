from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago

default_args = {
    "start_date": datetime(2024, 1, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="branch_shortcircuit_dag",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    def check_data_quality():
        # Simulate data quality check
        data_quality_ok = True
        return data_quality_ok

    def _choose_branch():
        # Simulate a decision for branching
        return "process_good_data"

    def process_good_data():
        print("Processing good data...")

    def handle_bad_data():
        print("Handling bad data...")

    check_quality = ShortCircuitOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=_choose_branch,
    )

    process_good_data_task = PythonOperator(task_id="process_good_data", python_callable=process_good_data)
    handle_bad_data_task = PythonOperator(task_id="handle_bad_data", python_callable=handle_bad_data)

    check_quality >> branch_task >> [process_good_data_task, handle_bad_data_task]
