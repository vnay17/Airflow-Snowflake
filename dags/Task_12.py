import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from snowflake.connector import connect
# from google.cloud import storage

# Define connection IDs for clarity
snowflake_conn_id = 'snowflake_conn'
gcs_conn_id = 'google_cloud_default'

with DAG(
    'snowflake_truncate_to_load_gcs',
    start_date=datetime(2024, 1, 17),
    schedule_interval=None,
) as dag:

    def truncate_and_load(**context):
        # Retrieve connection ID, prioritizing from context or using default
        snowflake_conn_id = context.get('snowflake_conn_id') or 'snowflake_conn'

        try:
            # Attempt to retrieve connection using Airflow's hook
            snowflake_conn = connect(
                **airflow.hooks.base.BaseHook.get_connection(snowflake_conn_id).get_uri().config
            )
        except AttributeError:
            # Fallback to manual connection if retrieval fails
            snowflake_conn = connect(
                user='VINAYKUMAR',  
                password='Vinay@1707',
                account='SF23033.us-east-2.aws',
                warehouse='COMPUTE_WH',
                database='SCHOOL',
                schema='PUBLIC'
            )

        # Truncate the specified table in Snowflake
        cursor = snowflake_conn.cursor()
        cursor.execute("TRUNCATE TABLE {}".format(context['table_name']))
        cursor.close()
        snowflake_conn.close()

    # Create the task using the PythonOperator
    truncate_and_load_task = PythonOperator(
        task_id='truncate_and_load',
        python_callable=truncate_and_load,
        provide_context=True,
        op_kwargs={
            'bucket_name': 'airflow-bqtogcs-bckt',
            'file_name': 'file.csv',
            'table_name': 'STUDENTS'
        }
    )
