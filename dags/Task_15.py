from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Configuration parameters (replace with your values)
snowflake_conn_id = "snowflake_conn"
table_with_value = "BRANCH"
value_column = "AGE"
target_table = "CUSTOMERS"
query_template = "SELECT * FROM CUSTOMERS WHERE AGE >= 25"  # Replace with your query

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="snowflake_parameterized_query_dag",
    default_args=default_args,
    schedule_interval=None,  # Set a schedule if desired
) as dag:

    get_value = SnowflakeOperator(
        task_id="get_value_from_table",
        sql=f"SELECT AGE FROM CUSTOMERS LIMIT 5",
        snowflake_conn_id=snowflake_conn_id,
    )

    execute_parameterized_query = SnowflakeOperator(
        task_id="execute_parameterized_query",
        sql=query_template,
        snowflake_conn_id=snowflake_conn_id,
        parameters={"value": "{{ task_instance.xcom_pull(task_ids='get_value_from_table', key='return_value')[0][0] }}"},
    )

    create_target_table = SnowflakeOperator(
        task_id="create_target_table",
        sql=f"CREATE TABLE IF NOT EXISTS students AS SELECT * FROM (CUSTOMERS)",
        snowflake_conn_id=snowflake_conn_id,
    )

    get_value >> execute_parameterized_query >> create_target_table
