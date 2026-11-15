"""MacroFlow ML batch forecasting DAG triggered after Lakehouse Feature Store & Training."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from airflow import DAG
from src.extractors.airflow_helpers import build_databricks_submit_kwargs, notify_task_failure

default_args = {
    "owner": "macroflow-mlops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_task_failure,
}

with DAG(
    dag_id="macroflow_ml_inference_batch",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,  # Triggered by macroflow_ingestion_bronze via TriggerDagRunOperator
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "lightgbm", "forecasting", "snowflake", "batch"],
) as dag:
    run_batch_scoring = DatabricksSubmitRunOperator(
        task_id="execute_lgbm_batch_scoring",
        **build_databricks_submit_kwargs(
            "ml/03_batch_predict.py",
            is_ml=True,
            parameters=["{{ dag_run.conf.get('logical_date', ds) }}"],
        ),
    )

    run_batch_scoring
