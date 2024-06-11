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

# Global connection to DuckDB
duckdb_conn = None

def connect_to_duckdb():
    """
    Establishes a connection to the DuckDB database.

    Returns:
        duckdb.DuckDBPyConnection: A connection object to the DuckDB database.

    Raises:
        Exception: If an error occurs during the connection attempt.

    """

    global duckdb_conn

    # Ensure a single connection instance is reused
    if duckdb_conn is None:
        try:
            # Connect to DuckDB
            duckdb_conn = duckdb.connect("duckdb/duckdb.db")
            logging.info("Successfully connected to DuckDB")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise
    
    return duckdb_conn

def get_src_tables():
    """
    Retrieves the names of source tables from a MSSQL database.

    Returns:
        list: A list containing the names of source tables.

    """

    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id="mssql_default")

    # Retrieve table names
    select_table_statement = "SELECT name AS table_name FROM eunomia.sys.tables"
    df = hook.get_pandas_df(select_table_statement)
    table_list = df['table_name'].tolist()

    return table_list

def extract_data_from_src(table_name):
    """
    Extracts data from a specified table in a MSSQL database.

    Args:
        table_name (str): The name of the table to extract data from.

    Returns:
        pandas.DataFrame: DataFrame containing the extracted data.
        
    """

    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    select_statement = f"SELECT * FROM {table_name}"
    logging.info(f"Extracting data from table: {table_name}")

    # Execute query and fetch data into a pandas DataFrame
    cursor.execute(select_statement)
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(data, columns=columns)

    # Close cursor and connection
    cursor.close()
    conn.close()
    
    return df

def create_duckdb_table(catalog_name, schema_name, table_name):
    """
    Creates a DuckDB table if it does not already exist and populates it with data from a source DataFrame.

    Args:
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table to create.

    """

    # Establishe a connection
    conn = connect_to_duckdb()

    # Get a list of existing DuckDB tables
    table_result = conn.execute(f"""SELECT table_name FROM information_schema.tables
                     WHERE table_catalog='{catalog_name}' AND table_schema='{schema_name}'""").fetchall()
    duckdb_tables =  [row[0] for row in table_result]

    # Check if the DuckDB table already exists
    if table_name not in duckdb_tables:
        df = extract_data_from_src(table_name)
    
        # Create the table
        create_table_statement = f"""CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} AS 
                                     SELECT * FROM df;"""
        conn.execute(create_table_statement)
        
        creation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Successfully created table: {table_name}. Created on: {creation_date}")
        
        # Commit changes
        conn.commit()
    else:
        logging.info(f"Table '{table_name}' already exists in catalog '{catalog_name}' and schema '{schema_name}'")

def load_src_data_to_duckdb(catalog_name, schema_name, table_name):
    """
    Loads source data into a DuckDB table.

    Args:
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table to load data into.
        
    """

    # Establishe a connection
    conn = connect_to_duckdb()

    # Create the table if not exists
    create_duckdb_table(catalog_name, schema_name, table_name)

    # Count the existing records in the table
    count_result = conn.execute(f"""SELECT count(*) FROM {catalog_name}.{schema_name}.{table_name};""").fetchall()
    count = count_result[0][0]
    logging.info(logging.info(f"Found {count} records from table {table_name}"))

    # If the table is empty:
    if count == 0:
        df = extract_data_from_src(table_name)

        # Insert data from source into DuckDB table
        conn.register('df', df)
        conn.execute(f"""INSERT INTO {catalog_name}.{schema_name}.{table_name}
                         SELECT * FROM df""")
        logging.info(f"Data inserted successfully into {table_name}")
    # If the table already has data:
    else:
        skip_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Skipping insert process as {table_name} already has data. Skipped on {skip_date}")

    # Close the connection
    conn.close()

with DAG('data_transfer', 
         default_args=default_args, 
         description='A DAG to transfer data from MSSQL to DuckDB',
         schedule_interval='@daily',
         catchup=False) as dag:

    src_tables = get_src_tables()
    previous_table_task = None
    
    for table in src_tables:
        with TaskGroup(f"table_{table}_group") as table_group:
            # Task to extract data from the source
            extract_task = PythonOperator(
                task_id=f'extract_data_{table}',
                python_callable=extract_data_from_src,
                op_kwargs={'table_name': table},
                dag=dag
            )

            # Task to load data into DuckDB
            load_task = PythonOperator(
                task_id=f'load_data_{table}',
                python_callable=load_src_data_to_duckdb,
                op_kwargs={'catalog_name': 'duckdb', 'schema_name': 'eunomia', 'table_name': table},
                dag=dag
            )
    
            extract_task >> load_task

        if previous_table_task:
            # Current table's task group starts only after the previous table's task group has completed
            previous_table_task >> table_group
        
        previous_table_task = table_group