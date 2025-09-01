from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def get_table_columns_from_postgres(conn_id, table_names):
    """
    Extracts column names for each table from PostgreSQL using Airflow's PostgresHook.
    
    Args:
        conn_id (str): The Airflow connection ID for the PostgreSQL database.
        table_names (list): A list of table names to get the schema for.

    Returns:
        dict: A dictionary where keys are table names and values are lists of column names.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    schema = {}
    
    for table in table_names:
        sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}' AND table_schema = 'public'
            ORDER BY ordinal_position;
        """ 
        try:
            result = hook.get_records(sql)
            column_names = [row[0] for row in result]   
            if column_names:
                schema[table] = column_names
                logging.info(f"Fetched schema for table '{table}': {column_names}")
            else:
                logging.warning(f"No columns found for table '{table}'. Check table name and schema.")
                schema[table] = []
                
        except Exception as e:
            logging.error(f"Failed to fetch schema for table '{table}': {e}")
            schema[table] = []

    return schema


def generate_merge_sql(table_name, table_config, columns, dataset, timestamp_column="updated_at_timestamp"):
    """
    Generates a BigQuery MERGE SQL statement with optional deduplication.

    Args:
        table_name (str): Name of the table.
        table_config (dict): Contains 'primary_keys' (list) and 'deduplicate' (bool).
        columns (list): List of column names.
        dataset (str): BigQuery dataset name.
        timestamp_column (str): Column used for deduplication ordering.

    Returns:
        str: A MERGE SQL statement.
    """
    primary_keys = table_config["primary_keys"]
    deduplicate = table_config.get("deduplicate", False)

    # Build ON clause
    on_clause = " AND ".join([f"T.{pk} = S.{pk}" for pk in primary_keys])

    # Build update and insert clauses
    update_clause = ",\n    ".join([f"T.{col} = S.{col}" for col in columns])
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"S.{col}" for col in columns])

    # Build source query
    if deduplicate:
        partition_by = ", ".join(primary_keys)
        source_query = f"""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {timestamp_column} DESC) AS rn
                FROM `{dataset}.{table_name}_staging_ayahany`
            )
            WHERE rn = 1
        """
        using_clause = f"USING ({source_query}) S"
    else:
        using_clause = f"USING `{dataset}.{table_name}_staging_ayahany` S"

    # Final MERGE SQL
    return f"""
        MERGE `{dataset}.{table_name}_ayahany` T
        {using_clause}
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET
            {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
    """