# data-Pipeline

## Overview

This repository contains an ETL (Extract, Transform, Load) pipeline that processes restaurant and review data. The pipeline is designed to ingest data from JSON and SQLite sources, transform the data, and load it into a Redshift database. Airflow is used to orchestrate the pipeline.

## Components

1. **Data Ingestion (`data_ingestion.py`)**
   - Loads restaurant data from a JSON file and review data from a SQLite database.
   
2. **Data Transformation (`data_transformation.py`)**
   - Cleans and transforms the ingested data.
   
3. **Data Loading (`data_loading.py`)**
   - Loads the cleaned data into a Redshift database.
   
4. **Airflow DAG (`data_pipeline_dag.py`)**
   - Orchestrates the ETL process and manages task dependencies.

## Requirements

- Python 3.x
- `pandas`, `numpy`, `requests`, `sqlalchemy`, `psycopg2` Python packages
- Airflow
- Access to a Redshift database
- Slack webhook URL for failure alerts

## Setup

1. **Install Dependencies:**
   Install the required Python packages using pip:
   ```sh
   pip install pandas numpy requests sqlalchemy psycopg2

**Running the Pipeline**

Start Airflow:
Start the Airflow web server and scheduler:

airflow webserver --port 8080
airflow scheduler

**Trigger the DAG:**

Open the Airflow web interface at http://localhost:8080.

Locate the data_pipeline DAG and trigger it manually or wait for the scheduled run.
