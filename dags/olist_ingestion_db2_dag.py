from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from ayahany.utils.schema_utils import get_table_columns_from_postgres, generate_merge_sql

# customers_db
POSTGRES_CONN_ID = "postgres_olist_db2_ayahany" 
GCS_BUCKET = "ready-labs-postgres-to-gcs"
BIGQUERY_DATASET = "ready-de26.project_landing" 
TABLES_DB2 = {
    "customers": "customer_id ",
    "geolocation": "geolocation_zip_code_prefix",
    "leads_closed": "mql_Id",
    "leads_qualified": "mql_Id"
}
TIMESTAMP_COLUMN = "updated_at_timestamp"

schema = get_table_columns_from_postgres(POSTGRES_CONN_ID, list(TABLES_DB2.keys()))
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    "olist_db2_incremental_ingestion_dag_ayahany",
    default_args=default_args,
    start_date=datetime(2025, 8, 23),
    end_date=datetime(2025, 8, 27),
    schedule_interval='@daily',
    catchup=True,
    tags=["ayahany", "db2_incremental"],

) as dag:
    for tbl, pk in TABLES_DB2.items():
        columns = schema[tbl]
        merge_sql = generate_merge_sql(tbl, pk, columns, BIGQUERY_DATASET)
        
        export = PostgresToGCSOperator(
            task_id=f"{tbl}_export_to_gcs",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f"""
                SELECT * FROM {tbl}
                WHERE {TIMESTAMP_COLUMN} >= '{{{{ macros.ds_add(ds, -1) }}}}'
                AND {TIMESTAMP_COLUMN} < '{{{{ ds }}}}'
            """,
            bucket=GCS_BUCKET,
            filename=f"db2_ayahany/{tbl}_ayahany/{{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json",
            export_format="json",
        )

       
        load_staging = GCSToBigQueryOperator(
            task_id=f"{tbl}_load_to_bq_staging",
            bucket=GCS_BUCKET,
            source_objects=[
                f"db2_ayahany/{tbl}_ayahany/{{{{ ds[:4] }}}}/{{{{ ds[5:7] }}}}/{{{{ ds[8:] }}}}/data.json"
            ],
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.{tbl}_staging_ayahany",
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )
        create_landing_if_missing = BigQueryInsertJobOperator(
            task_id=f"{tbl}_create_if_missing",
            configuration={
                "query": {
                    "query": f"""
                        CREATE SCHEMA IF NOT EXISTS `{BIGQUERY_DATASET}`;
                        CREATE TABLE IF NOT EXISTS `{BIGQUERY_DATASET}.{tbl}_ayahany`
                        AS SELECT * FROM `{BIGQUERY_DATASET}.{tbl}_staging_ayahany`
                        WHERE 1=0;
                    """,
                    "useLegacySql": False,
                }
            },
            location="US", 
        )
        
        merge = BigQueryInsertJobOperator(
            task_id=f"{tbl}_merge_to_main",
            configuration={
                "query": {
                    "query": merge_sql,
                    "useLegacySql": False,
                }
            },
        )

        export >> load_staging >> create_landing_if_missing >> merge  