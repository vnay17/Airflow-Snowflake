from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Configuration parameters (replace with your values)
snowflake_conn_id = "snowflake_conn"
snowflake_database = "BRANCH"
snowflake_schema = "PUBLIC"
snowflake_warehouse = 'COMPUTE_WH'
gcs_bucket_name = "airflow-bqtogcs-bckt"  # Used for error logging (optional)

# Default arguments for operators
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': datetime(2024, 1, 4),
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="snow_to_gcs_dag",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    
    load_table = SnowflakeOperator(
        task_id='load_Data',
        snowflake_conn_id=snowflake_conn_id,
        sql="""
               COPY INTO 'gcs://airflow-bqtogcs-bckt/bqtogcs/'
                FROM BRANCH.PUBLIC.EMPLOYEES
                STORAGE_INTEGRATION = SnowflaketoGcs
                FILE_FORMAT = (TYPE='CSV');
            """,
            dag=dag,
    )

