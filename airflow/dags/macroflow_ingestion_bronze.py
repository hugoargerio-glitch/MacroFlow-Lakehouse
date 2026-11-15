"""MacroFlow end-to-end Bronze ingestion, Databricks Lakehouse, and ML trigger DAG."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2.x compatibility
    from airflow.operators.python import PythonOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

try:
    from airflow.sdk import TaskGroup
except ImportError:  # Airflow 2.x compatibility
    from airflow.utils.task_group import TaskGroup

from src.extractors.airflow_helpers import (
    build_databricks_submit_kwargs,
    get_databricks_raw_base_path,
    notify_task_failure,
)
from src.extractors.bcb import BCB_SERIES_SPECS, coerce_exec_date, extract_bcb_series
from src.extractors.storage import upload_raw_partition
from src.extractors.yfinance_extractor import DEFAULT_YFINANCE_TICKERS, extract_yfinance_assets

RAW_BASE_PATH = os.environ.get("MACROFLOW_RAW_BASE_PATH", "/opt/airflow/raw_data")

default_args = {
    "owner": "macroflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_task_failure,
}


def run_bcb_extraction(series_code: int, series_name: str, logical_date: Any = None, **kwargs: Any) -> str | None:
    """Airflow callable wrapper for modular BCB SGS series extraction."""
    output_path = extract_bcb_series(
        series_code=series_code,
        series_name=series_name,
        logical_date=logical_date,
        raw_base_path=RAW_BASE_PATH,
    )
    return str(output_path) if output_path else None


def run_yfinance_extraction(tickers: list[str], logical_date: Any = None, **kwargs: Any) -> dict[str, Any]:
    """Airflow callable wrapper for modular Yahoo Finance extraction."""
    result = extract_yfinance_assets(
        tickers=tickers,
        logical_date=logical_date,
        raw_base_path=RAW_BASE_PATH,
        fail_on_partial=False,
    )
    return {
        "output_path": str(result.output_path) if result.output_path else None,
        "records_count": result.records_count,
        "succeeded_tickers": result.succeeded_tickers,
        "failed_tickers": result.failed_tickers,
    }


def run_upload_to_databricks(logical_date: Any = None, **kwargs: Any) -> list[str]:
    """Upload Hive-partitioned edge NDJSON files to Databricks UC Volumes / DBFS."""
    exec_date = coerce_exec_date(logical_date)
    return upload_raw_partition(
        exec_date=exec_date,
        local_base_path=RAW_BASE_PATH,
        remote_base_path=get_databricks_raw_base_path(),
    )


with DAG(
    dag_id="macroflow_ingestion_bronze",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 21 * * 1-5",  # Weekdays at 21:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "lakehouse", "shift-left", "bcb", "yfinance"],
) as dag:
    task_yfinance = PythonOperator(
        task_id="extract_yfinance_market_data",
        python_callable=run_yfinance_extraction,
        op_kwargs={"tickers": list(DEFAULT_YFINANCE_TICKERS)},
    )

    bcb_tasks = [
        PythonOperator(
            task_id=f"extract_bcb_{spec.name.lower()}",
            python_callable=run_bcb_extraction,
            op_kwargs={"series_code": spec.code, "series_name": spec.name},
        )
        for spec in BCB_SERIES_SPECS.values()
    ]

    upload_task = PythonOperator(
        task_id="upload_raw_to_databricks",
        python_callable=run_upload_to_databricks,
    )

    with TaskGroup(group_id="databricks_lakehouse_pipeline") as lakehouse_group:
        bronze_task = DatabricksSubmitRunOperator(
            task_id="run_bronze_ingestion",
            **build_databricks_submit_kwargs("01_bronze_ingestion.py"),
        )
        silver_task = DatabricksSubmitRunOperator(
            task_id="run_silver_cleaning",
            **build_databricks_submit_kwargs("02_silver_cleaning.py"),
        )
        gold_task = DatabricksSubmitRunOperator(
            task_id="run_gold_analytics",
            **build_databricks_submit_kwargs("03_gold_analytics.py"),
        )
        feature_store_task = DatabricksSubmitRunOperator(
            task_id="run_feature_store",
            **build_databricks_submit_kwargs("ml/01_feature_store.py", is_ml=True),
        )
        train_task = DatabricksSubmitRunOperator(
            task_id="run_train_and_register",
            **build_databricks_submit_kwargs("ml/02_train_and_register.py", is_ml=True),
        )
        snowflake_sync_task = DatabricksSubmitRunOperator(
            task_id="run_snowflake_gold_sync",
            **build_databricks_submit_kwargs("04_snowflake_sync.py"),
        )

        bronze_task >> silver_task >> gold_task >> [feature_store_task, snowflake_sync_task]
        feature_store_task >> train_task

    trigger_ml_inference = TriggerDagRunOperator(
        task_id="trigger_ml_inference_batch",
        trigger_dag_id="macroflow_ml_inference_batch",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"logical_date": "{{ ds }}"},
    )

    # All 4 edge extractors run in parallel, then upload, then Lakehouse Medallion + Snowflake Sync, then ML Batch Inference
    [*bcb_tasks, task_yfinance] >> upload_task >> lakehouse_group >> trigger_ml_inference
