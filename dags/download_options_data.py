from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from datetime import datetime, timedelta

# Constants for the Databricks job
DATABRICKS_CONN_ID: str = 'databricks'
NOTEBOOK_PATH = '/Workspace/Users/piaozhexiu@gmail.com/download options data'
CLUSTER_SPEC = {
    "cluster_name": "autoscaling-cluster",
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "rd-fleet.xlarge",
    "autoscale": {
        "min_workers": 2,
        "max_workers": 8
    },
    "aws_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "zone_id": "auto",
        "spot_bid_price_percent": 100,
        "ebs_volume_count": 0
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
    download_options_data = DatabricksNotebookOperator(
        task_id='download_options_data',
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_path=NOTEBOOK_PATH,
        new_cluster=CLUSTER_SPEC,
        source='WORKSPACE'
    )

    # End marker
    end = EmptyOperator(
        task_id='end'
    )

# Setting up the task pipeline
start << download_options_data << end
