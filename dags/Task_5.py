from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

def _validate_data():
    # Simulate some data validation logic
    data_valid = True  # Replace with your actual validation
    return "process_data" if data_valid else "handle_invalid_data"

with DAG(
    dag_id="branching_dag",
    default_args=default_args,
    schedule_interval=None,  # Manually trigger or by another DAG
) as dag:

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    branch_op = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=_validate_data,  # Same function for decision-making
    )

    process_data = DummyOperator(task_id="process_data")
    handle_invalid_data = DummyOperator(task_id="handle_invalid_data")

    validate_task >> branch_op >> [process_data, handle_invalid_data]
