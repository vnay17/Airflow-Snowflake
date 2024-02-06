from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

# Configuration parameters (replace with your values)
snowflake_conn_id = "snowflake_conn"
snowflake_database = "BRANCH"
snowflake_schema = "PUBLIC"
snowflake_table = "EMPLOYEES"
gcs_stage = "SNOWFLAKE"  # External stage pointing to GCS

# Default arguments for operators
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="conditional_load_gcs_to_snowflake_dag",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    check_table_empty = SnowflakeOperator(
        task_id="check_table_empty",
        sql=f"SELECT COUNT(*) FROM BRANCH.PUBLIC.EMPLOYEES",
        snowflake_conn_id=snowflake_conn_id,
    )

    def should_load_data(**context):
        task_instance = context["task_instance"]
        result = task_instance.xcom_pull(task_ids="check_table_empty")
        if result and result[0] == 0:
            return "load_data_from_gcs"  # Load if table is empty
        else:
            return "do_nothing"  # Skip if table is not empty

    load_data = SnowflakeOperator(
        task_id="load_data_from_gcs",
        sql=f"""
            COPY INTO BRANCH.PUBLIC.EMPLOYEES
            FROM @SNOWFLAKE
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = CONTINUE  -- Continue loading on error
        """,

        snowflake_conn_id=snowflake_conn_id,
    )

    do_nothing = DummyOperator(task_id="do_nothing")

# Tasks
    check_table_empty >> BranchPythonOperator(
    task_id="should_load_data", python_callable=should_load_data
    ) >> [load_data, do_nothing]

