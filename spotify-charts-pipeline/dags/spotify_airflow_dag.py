from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="spotify_charts_etl",
    default_args=default_args,
    description="Spotify Charts ETL pipeline (load, validate + report only)",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spotify", "etl"],
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(hours=4),
) as dag:

    # Run the loader script with unbuffered output
    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command="python -u /opt/airflow/dags/spotify_db_loader.py",
        execution_timeout=timedelta(hours=3),
    )

    # Run the validation script with unbuffered output
    validate = BashOperator(
        task_id="validate",
        bash_command="python -u /opt/airflow/dags/spotify_data_validation.py",
    )

    # Run the report script with unbuffered output
    report = BashOperator(
        task_id="report",
        bash_command="python -u /opt/airflow/dags/spotify_reporting.py",
    )

    load_postgres >> validate >> report
