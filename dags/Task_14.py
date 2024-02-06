from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Configuration parameters (replace with your values)
snowflake_account = "GQ85333.us-east-2.aws"
snowflake_user = "RANDYORTON"
snowflake_password = "Vinay@1707"
snowflake_database = "BRANCH"
snowflake_warehouse = "COMPUTE_WH"
snowflake_role = "ACCOUNTADMIN"
query = "SELECT * FROM BRANCH.PUBLIC.EMPLOYEES"  # Replace with your query
new_table_name = "CUSTOMERS"  # Replace with the desired table name
snowflake_conn_id = "snowflake_conn"

# Default arguments for operators
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="snowflake_query_to_new_table_dag",
    default_args=default_args,
    schedule_interval=None,  # Set a schedule if desired
) as dag:

    execute_query = SnowflakeOperator(
        task_id="execute_snowflake_query",
        sql=query,
        snowflake_conn_id="snowflake_conn",  # Replace with your connection ID
        autocommit=True,
    )

    create_table = SnowflakeOperator(
        task_id="create_new_table",
        sql=f"CREATE TABLE IF NOT EXISTS workers AS SELECT * FROM (BRANCH.PUBLIC.EMPLOYEES)",
        snowflake_conn_id="snowflake_conn",  # Replace with your connection ID
    )

    execute_query >> create_table
