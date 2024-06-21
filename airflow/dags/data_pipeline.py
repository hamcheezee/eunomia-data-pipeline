from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

catalog_name = "duckdb"
schema_name = "eunomia"
chunk_size = 10000

def get_src_tables(schema_name):
    """
    Retrieves the names of source tables from a MSSQL database.

    Args:
        schema_name (str): The name of the schema.

    Returns:
        list: A list containing the names of source tables.

    """

    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id="mssql_default")

    # Retrieve table names
    df = hook.get_pandas_df(f"SELECT name AS table_name FROM {schema_name}.sys.tables")
    table_list = df['table_name'].tolist()

    return table_list

def extract_data_from_src(table_name, chunk_size=chunk_size):
    """
    Extracts data from a specified table in a MSSQL database.

    Args:
        table_name (str): The name of the table to extract data from.
        chunk_size (int): The number of rows to include in each chunk.

    Yields:
        pandas.DataFrame: DataFrame containing a chunk of rows from the specified table.
        
    """

    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()

    logging.info(f"Extracting data from table: {table_name}")
    total_rows = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", conn).iloc[0, 0]
    logging.info(f"Total rows to be processed from {table_name}: {total_rows}")

    # Execute query and fetch data into a pandas DataFrame
    query = f"SELECT * FROM {table_name}"
    chunk_count = 0
    for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
        chunk_count += 1
        logging.info(f"Processing chunk {chunk_count} with {len(chunk)} rows from {table_name}")
        yield chunk  # Yield the chunk as a DataFrame
    logging.info(f"Finished processing all chunks from {table_name}")

    # Close the connection
    conn.close()

def load_src_data_to_duckdb(catalog_name, schema_name, table_name):
    """
    Loads source data into a DuckDB table.

    Args:
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table to load data into.
        
    """

    # Establish a connection
    try:
        # Connect to DuckDB
        conn = duckdb.connect("duckdb/duckdb.db")
        logging.info("Successfully connected to DuckDB")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    total_rows_inserted = 0
    for chunk in extract_data_from_src(table_name):
        # Create an empty table if it does not exist
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} AS 
                     SELECT * FROM chunk
                     WHERE 0=1;""")

        # Insert data from source into DuckDB table
        conn.register('chunk', chunk)
        conn.execute(f"""INSERT INTO {catalog_name}.{schema_name}.{table_name}
                     SELECT * FROM chunk""")
    logging.info(f"Data inserted successfully into {table_name}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()

with DAG('data_transfer', 
         default_args=default_args, 
         description='A DAG to transfer data from MSSQL to DuckDB',
         schedule_interval=None,
         catchup=False) as dag:

    src_tables = get_src_tables(schema_name)
    previous_table_task = None
    
    for table in src_tables:
        with TaskGroup(f"table_{table}_group") as table_group:
            # Task to load data into DuckDB
            load_task = PythonOperator(
                task_id=f'load_data_{table}',
                python_callable=load_src_data_to_duckdb,
                op_kwargs={'catalog_name': catalog_name, 'schema_name': schema_name, 'table_name': table},
                dag=dag
            )
    
            load_task

        if previous_table_task:
            # Current table's task group starts only after the previous table's task group has completed
            previous_table_task >> table_group
        
        previous_table_task = table_group