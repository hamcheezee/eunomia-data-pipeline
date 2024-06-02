import os
import pandas as pd
import logging

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'load_csv_to_mssql',
    default_args=default_args,
    description='A DAG to initialize data in MSSQL from a CSV file',
    schedule_interval=None,
)

def create_table():
    """
    Create SQL tables based on CSV files present in a directory.

    This function reads each CSV file in a specified directory, infers the SQL data types for columns, and creates SQL tables accordingly.

    """

    def infer_sql_type(column, dtype):
        """
        Infer the SQL data type based on the column name and data type.

        Args:
            column (str): The name of the column.
            dtype: The data type of the column.

        Returns:
            str: The corresponding SQL data type.

        """
        
        if "date" in column:
            if "datetime" in column:
                return 'DATETIME2'                  # Datetime2 for datetime columns
            else:
                return 'DATE'                       # Date for date columns
        elif "time" in column:
            return 'TIME'                           # Time for time columns
        elif pd.api.types.is_integer_dtype(dtype):
            return 'BIGINT'                         # Big integer for integer columns
        elif pd.api.types.is_float_dtype(dtype):
            return 'FLOAT'                          # Float for float columns
        else:
            return 'VARCHAR(255)'                   # Default to VARCHAR(255) for other types
        
    # Initialize connection
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Directory containing CSV files
    data_dir = "/opt/airflow/dags/data/"

    for file in os.listdir(data_dir):
        # Extract table name from file name
        table_name = file.split('.')[0].lower()

        # Read CSV into DataFrame
        df = pd.read_csv(os.path.join(data_dir, file))

        # Infer SQL data types for DataFrame columns
        column_types = {column: infer_sql_type(column, df[column].dtype) for column in df.columns}

        # Generate column definitions for CREATE TABLE statement
        columns_def = ",\n    ".join([f"{column} {col_type}" for column, col_type in column_types.items()])

        # Create SQL statement for table creation
        create_statement = f"""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{table_name}]') AND type in (N'U'))
            BEGIN
                CREATE TABLE [dbo].[{table_name}] (
                    {columns_def}
                );
            END;
        """

        # Execute the CREATE TABLE query
        logging.info(f"Successfully created table: {table_name}")
        cursor.execute(create_statement)

    # Commit changes and close connection
    conn.commit()
    conn.close()

def load_csv_to_mssql():
    """
    Load data from CSV files to MSSQL tables.

    This function iterates through CSV files in a specified directory, reads each file into a DataFrame, and inserts the data into corresponding MSSQL tables.

    """

    # Initialize MSSQL connection
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Directory containing CSV files
    data_dir = "/opt/airflow/dags/data/"

    for file in os.listdir(data_dir):
        # Extract table name from file name
        table_name = file.split('.')[0].lower()

        # Read CSV into DataFrame
        df = pd.read_csv(os.path.join(data_dir, file))

        # Fill NaN values with None
        df = df.where(pd.notnull(df), None)

        # Get list of columns
        columns = df.columns.tolist()

        # Create SQL INSERT statement
        insert_statement = f"""
            INSERT INTO [dbo].[{table_name}] ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
        """

        # Iterate over DataFrame rows and insert into the table
        for row in df.itertuples(index=False, name=None):
            # Replace 'nan' values with None
            row = [None if pd.isna(value) else value for value in row]
            cursor.execute(insert_statement, row)

        logging.info(f"Successfully inserted data into table: {table_name}")
    
    # Commit changes and close connection
    conn.commit()
    conn.close()

with DAG('load_csv_to_mssql', 
         default_args=default_args, 
         description='A DAG to initialize data in MSSQL from a CSV file',
         schedule_interval=None) as dag:

    # Task to create tables in MSSQL
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        dag=dag,
    )

    # Task to load data from CSV to MSSQL
    load_csv_task = PythonOperator(
        task_id='load_csv_to_mssql',
        python_callable=load_csv_to_mssql,
        dag=dag,
    )

    create_table_task >> load_csv_task