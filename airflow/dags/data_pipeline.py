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

def connect_to_duckdb():
    """
    Establishes a connection to a DuckDB database and retrieves schema information.
    
    This function retrieves a list of all existing schemas in the database (including created schemas).

    Raises:
        Exception: If an error occurs during connection or query execution.
    
    """

    try:
        # Connect to DuckDB
        conn = duckdb.connect("duckdb.db")
        
        # Retrieve schema information
        schemas = conn.execute("SELECT DISTINCT schema_name FROM information_schema.schemata;").fetchall()
        logging.info(f"Retrieved schema names: {schemas}")
        
        # Close the connection
        conn.close()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

with DAG('data_pipeline', 
         default_args=default_args, 
         description='A data pipeline to transfer data from MSSQL to DuckDB',
         schedule_interval=None) as dag:

    connect_db_task = PythonOperator(
        task_id='connect_to_duckdb',
        python_callable=connect_to_duckdb,
        dag=dag,
    )

    connect_db_task