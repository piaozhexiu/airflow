from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksNotebookOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from datetime import datetime, timedelta

# Constants for the Databricks job
DATABRICKS_CONN_ID: str = 'databricks'
NOTEBOOK_PATH = '/Workspace/Users/piaozhexiu@gmail.com/download options data'
CLUSTER_SPEC = {
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
        'download_options_data',
        default_args={
            'owner': 'cheolsoo',
            'start_date': datetime(2025, 2, 9),
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A DAG to download options data',
        schedule_interval='30 21 * * *',  # 9:30 PM UTC
        catchup=False,
) as dag:
    # Start marker
    start = EmptyOperator(
        task_id='start'
    )

    extract_text_from_tweets = DatabricksSqlOperator(
        task_id='extract_text_from_tweets',
        databricks_conn_id=DATABRICKS_CONN_ID,
        sql_endpoint_name='Serverless Starter Warehouse',
        sql='./sql/tickers_text.sql'
    )

    extract_tickers_from_text = DatabricksSqlOperator(
        task_id='extract_tickers_from_text',
        databricks_conn_id=DATABRICKS_CONN_ID,
        sql_endpoint_name='Serverless Starter Warehouse',
        sql='./sql/tickers_from_tweets.sql'
    )

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
start >> extract_text_from_tweets >> extract_tickers_from_text >> download_options_data >> end
