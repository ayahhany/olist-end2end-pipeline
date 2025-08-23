from airflow import DAG 
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from datetime import datetime, timedelta 


POSTGRES_CONN_ID = "postgres_olist_db1" 
GCS_BUCKET = "bucket_name"
BIGQUERY_DATASET = "project.dataset"
TABLES_DB1 = ["orders","order_items","order_reviews","products","product_category_name_translation"] 

 

default_args = {"retries": 1, 
                "retry_delay": timedelta(minutes=5)
            } 

 

with DAG( 
    "olist_ingestion_db1_dag_ayahany", 
    default_args=default_args, 
    # start_date=datetime(2024,1,1), 
    schedule_interval=None, 
    catchup=False, 
    tags=["olist_ayahany","db1"], 

) as dag: 
    for tbl in TABLES_DB1: 
        export = PostgresToGCSOperator( 
            task_id=f"{tbl}_postgres_to_gcs", 
            postgres_conn_id=POSTGRES_CONN_ID, 
            sql=f"SELECT * FROM {tbl};", 
            bucket=GCS_BUCKET, 
            filename=f"db1_ayahany/{tbl}_ayahany", 
            export_format="json",
        ) 
        load = GCSToBigQueryOperator( 
            task_id=f"{tbl}_gcs_to_bq", 
            bucket=GCS_BUCKET, 
            source_objects=[f"db1_ayahany/{tbl}_ayahany"], 
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.{tbl}_ayahany", 
            source_format="NEWLINE_DELIMITED_JSON", 
            write_disposition="WRITE_TRUNCATE", 
            create_disposition="CREATE_IF_NEEDED", 

        ) 
        export >> load 