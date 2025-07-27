from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.google.cloud.operators.bigquery import BigQueryToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import DummyOperator

default_args_val = {
    "depends_on_past": False,
    "start_date": datetime(2025,7,1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="Chicago-Taxi-Data-Analysis",
    default_args=default_args_val,
    schedule_interval=None,
    catchup=False
) as dag:
    
    start_task = DummyOperator(dag=dag, task_id="start")

    # export_public_dataset_to_local_gcs = BigQueryToGCSOperator(
    #     task_id="export_public_dataset_to_local_gcs",
    #     source_project_dataset_table="bigquery-public-data.chicago_taxi_trips.taxi_trips",
    #     destination_cloud_storage_uris=["gs://gcp-chicago-taxi-details/taxi_trip/stage_data/taxi_trips*.parquet"],
    #     compression='SNAPPY',
    #     export_format='PARQUET',
    #     gcp_conn_id='google_cloud_default',
    #     query="""
    #     select *
    #     from bigquery-public-data.chicago_taxi_trips.taxi_trips
    #     where date(timestamp(trip_Start_timestamp)) >= DATE_SUB((select date(timestamp(max(trip_Start_timestamp)))
    #     from bigquery-public-data.chicago_taxi_trips.taxi_trips), INTERVAL 3 MONTH)
    #     """
    # )

    end_task = DummyOperator(dag=dag, task_id="end")

    start_task >> end_task
    # start_task >> export_public_dataset_to_local_gcs >> end_task
    # export_public_dataset_to_local_gcs
