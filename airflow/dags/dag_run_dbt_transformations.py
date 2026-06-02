"""
dag_run_dbt_transformations.py

DAG 2: dbt Transformations

Runs dbt staging and mart models after raw files are loaded.

Schedule: daily at 07:00 UTC (runs after ingestion DAG at 06:00)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------------------------
# dbt project path inside the Airflow container
# This matches the volume mount in docker-compose.yml
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_run_dbt_transformations",
    description="Run dbt staging and mart models after raw data ingestion",
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 7 * * *",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transformation", "retail"],
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}",
        env={
            "POSTGRES_HOST": "postgres",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}",
        env={
            "POSTGRES_HOST": "postgres",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
        env={
            "POSTGRES_HOST": "postgres",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_run_staging >> dbt_run_marts >> dbt_test >> end