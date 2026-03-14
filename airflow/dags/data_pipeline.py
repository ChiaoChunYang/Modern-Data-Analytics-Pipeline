from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta
# Import the slack alert function from the custom plugin
from slack_operator import task_fail_slack_alert

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
}

# Define the DAG
with DAG(
    'enterprise_data_pipeline',
    default_args=default_args,
    description='End-to-end data pipeline for marketing analytics',
    schedule_interval='@daily',
    catchup=False,
    tags=['marketing', 'snowflake', 'dbt'],
) as dag:

    start_node = DummyOperator(task_id='start_pipeline')

    # Step 1: Raw Data Ingestion (Simulated via Snowflake Stored Procedures or COPY commands)
    ingest_raw_data = SnowflakeOperator(
        task_id='ingest_raw_marketing_data',
        snowflake_conn_id='snowflake_default',
        sql="""
            CALL RAW.PUBLIC.INGEST_LATEST_CAMPAIGN_DATA();
        """,
    )

    # Step 2: dbt Cloud Job execution (Assumes dbt Cloud is used for orchestration of models)
    # Alternatively, use DbtRunOperator if self-hosting dbt Core
    run_dbt_models = DbtCloudRunJobOperator(
        task_id='run_dbt_marts_marketing',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345,  # Replace with actual dbt job ID
        check_interval=60,
        timeout=3600,
    )

    # Step 3: Data Quality Checks (Pytest or Snowflake-based)
    run_data_quality_tests = SnowflakeOperator(
        task_id='validate_data_quality',
        snowflake_conn_id='snowflake_default',
        sql="""
            SELECT COUNT(*) FROM ANALYTICS.MARTS_MARKETING.FCT_CAMPAIGN_PERFORMANCE WHERE DAILY_SPEND < 0;
            -- Should return 0, otherwise fail DAG via check operator
        """,
    )

    end_node = DummyOperator(task_id='end_pipeline')

    # Define task dependencies
    start_node >> ingest_raw_data >> run_dbt_models >> run_data_quality_tests >> end_node
