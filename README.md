# Eunomia Data Pipeline

Data orchestration system built on Apache Airflow to integrate and manage data sourced from the [Eunomia dataset](https://github.com/OHDSI/EunomiaDatasets).

## Getting Started

### Prerequisites
Before setting up the data pipeline with Airflow, ensure you have the following prerequisites installed:

- Docker
- Python (recommended version 3.12+)
- Database (in this case, MSSQL and DuckDB) for Airflow's metadata storage
- Optional: Any additional dependencies specific to your workflow tasks (e.g., database drivers, API clients)

### Installation

1. Create and activate a Python virtual environment
   
   ```
   python3 -m venv .env
     ```
   - Activate the virtual environment:
     - On Windows:
       ```
       .env\Scripts\activate
       ```
     - On macOS and Linux:
       ```
       source .env/bin/activate
       ```
       
2. Install Apache Airflow and Dependencies
   
    - Once your virtual environment is activated, use pip to install dependencies specified in the         ```requirements.lock``` file:
       ```
       pip install -r requirements.lock
       ```
    - Make sure to install any other dependencies required by your specific workflow tasks using pip.
  
3. Set up the Airflow environment
   
   - Export the environment variables with your own values:
       ```
       export MSSQL_USER=<MSSQL_USER>
       export MSSQL_SA_PASSWORD=<MSSQL_SA_PASSWORD>
       export MSSQL_DATABASE=<MSSQL_DATABASE>
       export MSSQL_PORT=<MSSQL_PORT>
       ```

    - Inside ```airflow/```, instead of manually installing dependencies, run the following make command to automate the process:
       ```
       make init
       ```
       > Make sure that the environment variables in the services section of the ```docker-compose.yaml``` file is replaced with your own environment variables. For example:
         > ```yaml
         > services:
         >  mssql:
         >    environment:
         >      - MSSQL_SA_PASSWORD=<MSSQL_SA_PASSWORD>
         > ```

4. Start Airflow services
   
    ```
    make run
    ```
    > Open a web browser and go to http://localhost:8080/ to access the Airflow web interface. Use the username ```airflow``` and password ```airflow``` to log in.

   #### Create connection ID
   Follow [this guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui) to create a connection ID in Airflow for storing connections in the database.

   > <img width="1408" alt="Screenshot 2567-06-03 at 00 09 09" src="https://github.com/hamcheezee/eunomia-data-pipeline/assets/135502061/3943036d-ab57-468b-8d39-cc9247e1ce62">
   > If you encounter an error while testing the connection, consider using the IP address instead of the host name. You can obtain the IP address associated with the MSSQL container by executing the following command:
   >
   > ```
   > docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <CONTAINER_NAME>
   > ```

