from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Variable.get("GCP_CREDENTIALS")
print("GOOGLE_APPLICATION_CREDENTIALS:", Variable.get("GCP_CREDENTIALS"))
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.docker.operators.docker import DockerOperator

GCS_BUCKET_NAME = r"gcp-chicago-taxi-details/taxi_trip/stage_data"
GCS_BASE_PATH = r"taxi_trips*"
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = Variable.get("GCP_DATASET")
BIGQUERY_TABLE = "raw_taxi_trips_data_dtls"
# PARTITIONING_COLUMN = "date(timestamp(trip_start_timestamp))"
PARTITIONING_COLUMN = "trip_start_timestamp"
public_dataset = "bigquery-public-data.chicago_taxi_trips.taxi_trips"


default_args_val = {
    "depends_on_past": False,
    "start_date": datetime(2025,7,1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="Chicago-Taxi-Data-Analysis",
    default_args=default_args_val,
    schedule=None,
    catchup=False
) as dag:
    
    start_task = EmptyOperator(dag=dag, task_id="start")

    export_public_dataset_to_local_gcs = BigQueryToGCSOperator(
        task_id="export_public_dataset_to_local_gcs",
        source_project_dataset_table=public_dataset,
        destination_cloud_storage_uris=[f"gs://{GCS_BUCKET_NAME}/{GCS_BASE_PATH}.parquet"],
        compression='SNAPPY',
        export_format='PARQUET',
        print_header=True,
        dag=dag, 
        project_id=GCP_PROJECT_ID,
    )

    load_parquet_to_bigquery = BigQueryInsertJobOperator(
        task_id="load_parquet_to_bigquery",
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET_NAME}/{GCS_BASE_PATH}.parquet"], 
                # "sourceUris": [f"gs://{GCS_BUCKET_NAME}/taxi_trips000000000000.parquet"],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": BIGQUERY_TABLE,
                },
                "sourceFormat": "PARQUET",
                "autodetect": True, 
                "writeDisposition": "WRITE_APPEND", 
                # "timePartitioning": {
                #     "type": "DAY", 
                #     "field": PARTITIONING_COLUMN,
                #     "requirePartitionFilter": True,
                # },
            }
        },
        gcp_conn_id="google_cloud_default",
        project_id=GCP_PROJECT_ID, 
    )

    # export_data_to_gcs = BigQueryInsertJobOperator(
    #     task_id="export_data_to_gcs",
    #     project_id='gcp-devspace-coder',  # Destination project for the new table
    #     configuration={
    #         "query": {
    #             "query": f"""
    #                 select *
    #                 from bigquery-public-data.chicago_taxi_trips.taxi_trips
    #                 where date(timestamp(trip_Start_timestamp)) >= DATE_SUB((select date(timestamp(max(trip_Start_timestamp)))
    #                 from bigquery-public-data.chicago_taxi_trips.taxi_trips), INTERVAL 3 MONTH)
    #             """,
    #             "useLegacySql": False,
    #             "destinationTable": {
    #                 "projectId": "gcp-devspace-coder",
    #                 "datasetId": "taxi_trips_dataset",
    #                 "tableId": "taxi_trips_data",
    #             },
    #             "createDisposition": "CREATE_IF_NEEDED",
    #             "writeDisposition": "WRITE_TRUNCATE",
    #         }
    #     },
    # )

    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-bigquery:1.9.latest',
        container_name='airflow_dbt_run',
        auto_remove='success',
        command="dbt run --profiles-dir /usr/app/dbt",
        docker_url="unix:///var/run/docker.sock",
        network_mode="time-dotcom_api-networks",
        working_dir='/usr/app/dbt',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=r"/home/msgrace/workspace/dev-space/de-project/chicago-taxi-trip/analytics-1/time-dotcom/dbt_taxi_data",
                target=r"/usr/app/dbt",
                type="bind"
            ),
            Mount(
                source=r"/home/msgrace/workspace/dev-space/de-project/chicago-taxi-trip/analytics-1/time-dotcom/dbt_taxi_data/profiles.yml",
                target="/usr/app/dbt/profiles.yml",
                type="bind"
            )

        ],
        dag=dag,
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        image='ghcr.io/dbt-labs/dbt-bigquery:1.9.latest',
        command="dbt test --profiles-dir /usr/app/dbt",
        dag=dag,
    )

    end_task = EmptyOperator(dag=dag, task_id="end")

    # start_task >> end_task
    # start_task >> export_data_to_gcs >> end_task
    # start_task >> export_public_dataset_to_local_gcs >> end_task
    start_task >> export_public_dataset_to_local_gcs >> load_parquet_to_bigquery
    load_parquet_to_bigquery >> dbt_run >> dbt_test >> end_task
    # export_public_dataset_to_local_gcs
