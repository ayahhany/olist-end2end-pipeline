from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging


def map_postgres_to_bigquery_type(pg_type):
    mapping = {
        "integer": "INT64",
        "bigint": "INT64",
        "smallint": "INT64",
        "serial": "INT64",
        "text": "STRING",
        "varchar": "STRING",
        "character varying": "STRING",
        "boolean": "BOOL",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMP",
        "date": "DATE",
        "numeric": "NUMERIC",
        "double precision": "FLOAT64",
        "real": "FLOAT64",
    }
    return mapping.get(pg_type.lower(), "STRING")  

def generate_create_table_sql(table_name, columns_with_types, dataset):
    column_defs = ",\n  ".join([
        f"{col} {map_postgres_to_bigquery_type(dtype)}"
        for col, dtype in columns_with_types
    ])
    return f"""
        CREATE TABLE IF NOT EXISTS `{dataset}.{table_name}_ayahany` (
          {column_defs}
        )
    """

def get_table_columns_from_postgres(conn_id, table_names):
    """
    Extracts column names for each table from PostgreSQL using Airflow's PostgresHook.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    schema = {}
    for table in table_names:
        sql = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}' AND table_schema = 'public'
            ORDER BY ordinal_position;
        """ 
        try:
            result = hook.get_records(sql)
            schema[table] = [(row[0], row[1]) for row in result]
            logging.info(f"Fetched schema for table {table}: {schema[table]}")
        except Exception as e:
            logging.error(f"Failed to fetch schema for table {table}: {e}")
            schema[table] = []

    return schema

def generate_merge_sql(table_name, primary_key, columns, dataset):
    """
    Generates a BigQuery MERGE SQL statement using the provided column list.
    """
    update_clause = ",\n    ".join([f"T.{col} = S.{col}" for col in columns])
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"S.{col}" for col in columns])

    return f"""
        MERGE `{dataset}.{table_name}_ayahany` T
        USING `{dataset}.{table_name}_staging_ayahany` S
        ON T.{primary_key} = S.{primary_key}
        WHEN MATCHED THEN
          UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
    """