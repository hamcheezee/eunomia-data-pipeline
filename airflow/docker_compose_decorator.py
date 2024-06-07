import os

from ruamel.yaml import YAML

DOCKER_COMPOSE_YAML = "docker-compose.yaml"

yaml = YAML(typ="safe")
yaml.default_flow_style = False

# Get MSSQL connection details from environment variables
mssql_user = os.environ.get("MSSQL_USER")
mssql_password = os.environ.get("MSSQL_SA_PASSWORD")
mssql_database = os.environ.get("MSSQL_DATABASE")
mssql_port = os.environ.get("MSSQL_PORT")

# Check if the mssql_ip.txt file exists
if os.path.exists("mssql_ip.txt"):
    # Read the MSSQL IP address from the file
    with open("mssql_ip.txt", "r") as f:
        mssql_ip = f.read().strip()
else:
    # Set mssql_ip to localhost if the file doesn't exist
    mssql_ip = "localhost"

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
        docker_compose["services"][service]["environment"]["AIRFLOW_CONN_MSSQL_DEFAULT"] = f"mssql://{mssql_user}:{mssql_password}@{mssql_ip}:{mssql_port}/{mssql_database}".replace("localhost", mssql_ip)
        # Add the DuckDB volume mapping
        if "${AIRFLOW_PROJ_DIR:-.}/duckdb:/app/duckdb" not in  docker_compose["services"][service]["volumes"]:
            docker_compose["services"][service]["volumes"].append("${AIRFLOW_PROJ_DIR:-.}/duckdb:/app/duckdb")

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

# Add duckdb-volume in volumes section
if "duckdb-volume" not in docker_compose["volumes"]:
    docker_compose["volumes"]["duckdb-volume"] = None

# Save the updated Docker Compose file
with open(DOCKER_COMPOSE_YAML, "w", encoding="utf-8") as fd:
    yaml.dump(docker_compose, fd)