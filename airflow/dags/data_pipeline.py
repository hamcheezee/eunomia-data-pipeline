from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
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

mssql_conn_id="mssql_default"
conn_id="mssql_default"
mssql_database_name = "eunomia"
mssql_schema_name = "dbo"
duckdb_catalog_name = "duckdb"
duckdb_schema_name = "eunomia"
chunk_size = 10000

def get_src_tables(database_name):
    """
    Retrieves the names of source tables from a MSSQL database.

    Args:
        database_name (str): The name of the database.

    Returns:
        list: A list containing the names of source tables.

    """

    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

    # Retrieve table names
    df = hook.get_pandas_df(f"SELECT name AS table_name FROM {database_name}.sys.tables")
    table_list = df['table_name'].tolist()

    return table_list

def get_src_table_types(table_name):
    """
    Retrieves column names and their corresponding data types from a specified table in MSSQL database.

    Args:
        table_name (str): The name of the table.

    Returns:
        dict: A dictionary mapping column names to their respective data types.
              Example: {'column1': 'varchar', 'column2': 'int', ...}
    """
    
    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

    # Retrieve column names and their types
    df = hook.get_pandas_df(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
    column_types = {}
    column_types = df.set_index('column_name').to_dict()['data_type']
    column_types = {col: ('timestamp' if dtype == 'datetime2' else dtype) for col, dtype in column_types.items()}  # Replacing 'datetime2' with 'timestamp'

    return column_types

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
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()

    logging.info(f"Extracting data from table: {table_name}")
    total_rows = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", conn).iloc[0, 0]
    total_rows_processed = 0

    # Execute query and fetch data into a pandas DataFrame
    query = f"SELECT * FROM {table_name}"
    for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
        total_rows_processed += len(chunk)
        logging.info(f"Processing {total_rows_processed} out of {total_rows} rows from {table_name}")
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

    column_types = get_src_table_types(table_name)
    first_chunk = True
    for chunk in extract_data_from_src(table_name):
        if first_chunk:
            # Create an empty table if it does not exist
            query = f"CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (" + \
                     ", ".join([f"{col} {column_types[col]}" for col in chunk.columns]) + \
                     ")"
            conn.execute(query)
            first_chunk = False

        # Insert data from source into DuckDB table
        conn.register('chunk', chunk)
        conn.execute(f"""INSERT INTO {catalog_name}.{schema_name}.{table_name}
                     SELECT * FROM chunk""")
    logging.info(f"Data inserted successfully into {table_name}")

    # Close the connection
    conn.close()

with DAG('data_transfer', 
         default_args=default_args, 
         description='A DAG to transfer data from MSSQL to DuckDB',
         schedule_interval=None,
         catchup=False) as dag:

    src_tables = get_src_tables(mssql_database_name)
    previous_table_task = None
    
    for table_name in src_tables:
        with TaskGroup(f"table_{table_name}_group") as table_group:
            # Task to validate the data using Great Expectations
            validate_data_task = GreatExpectationsOperator(
                task_id=f'gx_validate_{table_name}',
                conn_id=conn_id,
                data_context_root_dir='/app/great_expectations',
                schema=mssql_schema_name,
                data_asset_name=table_name,
                expectation_suite_name=f'{table_name}_suite',
                return_json_dict=True,
                dag=dag,
            )

            # Task to load data into DuckDB
            load_task = PythonOperator(
                task_id=f'load_data_{table_name}',
                python_callable=load_src_data_to_duckdb,
                op_kwargs={'catalog_name': duckdb_catalog_name, 'schema_name': duckdb_schema_name, 'table_name': table_name},
                dag=dag
            )
    
            validate_data_task >> load_task

        if previous_table_task:
            # Current table's task group starts only after the previous table's task group has completed
            previous_table_task >> table_group
        
        previous_table_task = table_group