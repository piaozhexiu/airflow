from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

# Constants for the Databricks job
DATABRICKS_CONN_ID = 'databricks'
NOTEBOOK_PATH = '/Workspace/Users/piaozhexiu@gmail.com/download options data'
CLUSTER_SPEC = {
    "autoscale": {
        "max_workers": 8,
        "min_workers": 2
    },
    "autotermination_minutes": 60,
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        "ebs_volume_count": 0,
        "first_on_demand": 1,
        "spot_bid_price_percent": 100,
        "zone_id": "auto"
    },
    "cluster_name": "Transient Cluster For Airflow",
    "data_security_mode": "SINGLE_USER",
    "enable_elastic_disk": True,
    "node_type_id": "rd-fleet.xlarge",
    "runtime_engine": "PHOTON",
    "single_user_name": "piaozhexiu@gmail.com",
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    }
}

# DAG definition
with DAG(
        'databricks_notebook_invocation',
        default_args={
            'owner': 'cheolsoo',
            'start_date': datetime(2025, 2, 9),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A DAG to invoke a Databricks notebook',
        schedule_interval='@daily',
        catchup=False,
) as dag:
    # Start marker
    start = EmptyOperator(
        task_id='start'
    )

    # Task to run the Databricks notebook
    download_options_data = DatabricksRunNowOperator(
        task_id='download_options_data',
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=CLUSTER_SPEC
    )

    # End marker
    end = EmptyOperator(
        task_id='end'
    )

# Setting up the task pipeline
start << download_options_data << end
