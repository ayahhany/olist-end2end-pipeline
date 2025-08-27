# ✅ Starts on August 22, 2025
# ✅ Runs daily and fetches data from the previous day
# ✅ Exports to GCS with a hierarchical date structure
# ✅ Loads into a staging table in BigQuery
# ✅ Merges into a main BigQuery table

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# orders_products_db
POSTGRES_CONN_ID = "postgres_olist_db1_ayahany" 
GCS_BUCKET = "ready-labs-postgres-to-gcs"
BIGQUERY_DATASET = "ready-de26.project_landing" 
TABLES_DB1 = {
    "orders": "order_id",
    "order_items": "order_item_id",
    "order_reviews": "review_id",
    "products": "product_id",
    "product_category_name_translation": "product_category_name"
}
TIMESTAMP_COLUMN = "updated_at_timestamp"
 

default_args = {"retries": 1, 
                "retry_delay": timedelta(minutes=5)
            } 


with DAG(
    "olist_incremental_ingestion_db1_dag_ayahany",
    default_args=default_args,
    start_date=datetime(2025, 8, 21),
    end_date=datetime(2025, 8, 28),
    schedule_interval= '0 17 * * *', # Daily at 17:00 UTC (8 pm in cairo )
    catchup=True,
    tags=["olist_ayahany", "db1", "incremental"],
) as dag:
    for tbl, pk in TABLES_DB1.items():
        export = PostgresToGCSOperator(
            task_id=f"{tbl}_export_to_gcs",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f"""
                SELECT * FROM {tbl}
                WHERE {TIMESTAMP_COLUMN} BETWEEN '{{{{ macros.ds_add(ds, -1) }}}}' AND '{{{{ ds }}}}';
            """,
            bucket=GCS_BUCKET,
            filename=f"db1_ayahany/{tbl}_ayahany/{{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json",
            export_format="json",
        )

        load_staging = GCSToBigQueryOperator(
            task_id=f"{tbl}_load_to_bq_staging",
            bucket=GCS_BUCKET,
            source_objects=[
                f"db1_ayahany/{tbl}_ayahany/{{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json"
            ],
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.{tbl}_staging_ayahany",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )
        

        merge = BigQueryInsertJobOperator(
            task_id=f"{tbl}_merge_to_main",
            configuration={
                "query": {
                    "query": f"""
                        MERGE `{BIGQUERY_DATASET}.{tbl}_ayahany` T
                        USING `{BIGQUERY_DATASET}.{tbl}_staging_ayahany` S
                        ON T.{pk} = S.{pk}
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT ROW
                    """,
                    "useLegacySql": False,
                }
            },
        )
        export >> load_staging >> merge  