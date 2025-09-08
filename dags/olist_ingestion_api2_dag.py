import logging
from datetime import datetime
import requests
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_CONN_ID = "google_cloud_default"
GCS_BUCKET = "ready-labs-postgres-to-gcs"
BIGQUERY_DATASET = "ready-de26.project_landing" 
BQ_TABLE = "sellers_ayahany"
GCS_FILE_PATH = "api2_ayahany/sellers.csv" 


def fetch_api_data_and_save():
    api_url = "https://sellers-table-834721874829.europe-west1.run.app"
    try:
        response = requests.get(api_url)
        response.raise_for_status() 
        csv_content = response.text
        file_path = "/tmp/sellers.csv"
        with open("sellers.csv", "w", encoding="utf-8", newline='') as f:
            f.write(csv_content)
        logging.info(f"Successfully fetched CSV and saved to {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CSV from API: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise e


def upload_to_gcs(local_file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    gcs_object = GCS_FILE_PATH
    blob = bucket.blob(gcs_object)
    logging.info(f"Uploading file '{local_file_path}' to GCS bucket '{GCS_BUCKET}' as '{gcs_object}'")
    blob.upload_from_filename(local_file_path)
    logging.info("Upload to GCS completed.")
    return gcs_object


with DAG(
    dag_id="olist_api2_ingestion_dag_ayahany",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 25),
    catchup=False,
    tags=["api2", "ayahany"],
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id="fetch_data_from_api",
        python_callable=fetch_api_data_and_save,
    )

    upload_file_task = PythonOperator(
        task_id="upload_file_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={"local_file_path": "{{ task_instance.xcom_pull(task_ids='fetch_data_from_api') }}"},
    )

   
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["{{ task_instance.xcom_pull(task_ids='upload_file_to_gcs') }}"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BQ_TABLE}",
        source_format="CSV",  
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    fetch_data_task >> upload_file_task >> load_gcs_to_bq



