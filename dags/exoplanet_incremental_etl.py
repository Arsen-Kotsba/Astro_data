from __future__ import annotations

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


def run_extract_and_merge():
    from scripts.etl import run_etl

    run_etl()


def run_data_quality_checks():
    from scripts.etl import get_engine
    from sqlalchemy import text

    engine = get_engine()
    with engine.connect() as conn:
        duplicate_check = conn.execute(
            text(
                """
                SELECT COUNT(*) AS duplicate_count
                FROM (
                    SELECT pl_name
                    FROM dim_exoplanets
                    GROUP BY pl_name
                    HAVING COUNT(*) > 1
                ) t
                """
            )
        ).scalar_one()

        if duplicate_check > 0:
            raise ValueError(f"Found duplicate pl_name values: {duplicate_check}")


def finalize_log():
    # Логирование успешного завершения делается в scripts/etl.py.
    # Отдельный task оставлен для расширения (notifications, SLA, metrics).
    return True


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="exoplanet_incremental_etl",
    default_args=default_args,
    description="Incremental ETL from NASA Exoplanet Archive to MS SQL",
    schedule="0 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["exoplanets", "mssql", "incremental"],
) as dag:
    extract_to_stage = PythonOperator(
        task_id="extract_to_stage",
        python_callable=run_extract_and_merge,
    )

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    finalize = PythonOperator(
        task_id="finalize_log",
        python_callable=finalize_log,
    )

    extract_to_stage >> data_quality_checks >> finalize
