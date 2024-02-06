from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

default_args = {
    "start_date": datetime(2024, 1, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    'bigquery_to_gcs_operator',
    default_args=default_args,
    schedule_interval=None,
) as dag:
    
    export_data = BigQueryToGCSOperator(
        task_id='export_data',
        source_project_dataset_table='gcp-project-408806.bqtogcs.bqtogcs_table',
        destination_cloud_storage_uris=['gs://airflow-bqtogcs-bckt/file.csv'],
       
    )
