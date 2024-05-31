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
   
   - Export the `SA_PASSWORD` environment variable with your MSSQL password:
       ```
       export SA_PASSWORD=<SA_PASSWORD>
       ```

    - Inside ```airflow/```, instead of manually installing dependencies, run the following make command to automate the process:
       ```
       make init
       ```

4. Start Airflow services
   
    ```
    make run
    ```
    > Open a web browser and go to http://localhost:8080/ to access the Airflow web interface. Use the username ```airflow``` and password ```airflow``` to log in.
