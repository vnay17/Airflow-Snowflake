from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Define your Snowflake connection parameters
snowflake_conn_id = 'snowflake_conn'
database = 'BRANCH'
warehouse = 'COMPUTE_WH'
schema = 'PUBLIC'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG
dag = DAG(
    'execute_snowflake_procedure',
    default_args=default_args,
    description='DAG to execute a Snowflake stored procedure',
    schedule_interval='@daily',  # Run daily, adjust as needed
)

# Snowflake stored procedure task
execute_procedure_task = SnowflakeOperator(
    task_id='execute_procedure_task',
    sql="CALL stored_procedure();",  # Replace with your procedure name
    snowflake_conn_id=snowflake_conn_id,
    warehouse=warehouse,
    database=database,
    schema=schema,
    dag=dag,
)

# Log the returned value
log_returned_value_task = SnowflakeOperator(
    task_id='log_returned_value_task',
    sql="SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
    snowflake_conn_id=snowflake_conn_id,
    warehouse=warehouse,
    database=database,
    schema=schema,
    dag=dag,
)

# Set up task dependencies
execute_procedure_task >> log_returned_value_task

if __name__ == "__main__":
    dag.cli()
