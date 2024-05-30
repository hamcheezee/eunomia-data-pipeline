AIRFLOW_VERSION ?= 2.9.1
AIRFLOW_IMAGE_NAME ?= eunomia-pipeline
AIRFLOW_UID ?= $(shell id -u)

download-docker-compose:
		# Download the Docker Compose file
		curl -LfO 'https://airflow.apache.org/docs/apache-airflow/$(AIRFLOW_VERSION)/docker-compose.yaml'

decorate-docker-compose: download-docker-compose
		# Run the Python script to decorate the downloaded docker-compose.yaml file
		python3 ./docker_compose_decorator.py

download-cli:
		# Download the Airflow CLI
		curl -LfO 'https://airflow.apache.org/docs/apache-airflow/$(AIRFLOW_VERSION)/airflow.sh' && chmod +x airflow.sh

init-folders:
		# Initialize required directories
		mkdir -p ./dags ./logs ./plugins ./config

init-airflow-dockerfile:
	export AIRFLOW_VERSION=$(AIRFLOW_VERSION) AIRFLOW_UID=$(AIRFLOW_UID) && cat Dockerfile.template | envsubst '$$AIRFLOW_VERSION,$$AIRFLOW_UID' > Dockerfile		

build-airflow-image: init-airflow-dockerfile
		# Build the Airflow image
		docker build -t $(AIRFLOW_IMAGE_NAME) -f ./Dockerfile .

init-airflow: decorate-docker-compose
		export AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose up --force-recreate airflow-init

init-db:
		# Migrate Airflow's metadata database
		airflow db init

init: decorate-docker-compose download-cli init-folders build-airflow-image init-airflow init-db

run: build-airflow-image
		export AIRFLOW_IMAGE_NAME=$(AIRFLOW_IMAGE_NAME) AIRFLOW_UID=$(AIRFLOW_UID) && docker-compose up --force-recreate -d

stop:
		docker-compose down