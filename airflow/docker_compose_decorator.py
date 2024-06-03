import os

from ruamel.yaml import YAML

DOCKER_COMPOSE_YAML = "docker-compose.yaml"

yaml = YAML(typ="safe")
yaml.default_flow_style = False

# Load the Docker Compose file
with open(DOCKER_COMPOSE_YAML, "r", encoding="utf-8") as fd:
    docker_compose = yaml.load(fd)

# Update environment variables to disable loading Airflow example DAGs
if "x-airflow-common" in docker_compose:
    docker_compose["x-airflow-common"].setdefault("environment", {})
    docker_compose["x-airflow-common"]["environment"]["AIRFLOW_CORE_LOAD_EXAMPLES"] = "false"

for service in ["airflow-cli", "airflow-init", "airflow-scheduler", "airflow-worker"]:
    if "services" in docker_compose and service in docker_compose["services"]:
        docker_compose["services"][service].setdefault("environment", {})
        # Disable loading Airflow example DAGs
        docker_compose["services"][service]["environment"]["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
        # Allow test connection
        docker_compose["services"][service]["environment"]["AIRFLOW__CORE__TEST_CONNECTION"] = "Enabled"
        # Set the default MSSQL connection
        docker_compose["services"][service]["environment"]["AIRFLOW_CONN_MSSQL_DEFAULT"] = os.environ.get("AIRFLOW_CONN_MSSQL_DEFAULT")

# Set Postgres port mapping
if "services" in docker_compose and "postgres" in docker_compose["services"]:
    docker_compose["services"]["postgres"]["ports"] = ["5432:5432"]

# Add MSSQL service configuration
docker_compose["services"]["mssql"] = {
    "image": "mcr.microsoft.com/azure-sql-edge",
    "container_name": "mssql",
    "environment": {
        "ACCEPT_EULA": "1",
        "MSSQL_SA_PASSWORD": os.environ.get("MSSQL_SA_PASSWORD"),
        "MSSQL_PID": "Developer",
        "MSSQL_USER": "SA"
    },
    "ports": ["1433:1433",],
    "volumes": ["${AIRFLOW_PROJ_DIR:-.}/dags/data:/opt/airflow/dags/data",],
}

# Add DuckDB service configuration
docker_compose["services"]["duckdb"] = {
    "image": "duckdb",
    "container_name": "duckdb",
    "ports": ["5001:5000",],
}

# Save the updated Docker Compose file
with open(DOCKER_COMPOSE_YAML, "w", encoding="utf-8") as fd:
    yaml.dump(docker_compose, fd)