"""Shared Airflow DAG helpers for Databricks submission specs and failure alerting."""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping, Optional

logger = logging.getLogger(__name__)

DEFAULT_DATABRICKS_SCRIPTS_BASE_PATH = "/Volumes/macroflow/artifacts/scripts/databricks"
DEFAULT_DATABRICKS_RAW_BASE_PATH = "/Volumes/macroflow/bronze/raw_data"
DEFAULT_DATABRICKS_SPARK_VERSION = "14.3.x-cpu-ml-scala2.12"
DEFAULT_DATABRICKS_NODE_TYPE = "i3.xlarge"

DEFAULT_ML_LIBRARIES: tuple[dict[str, dict[str, str]], ...] = (
    {"pypi": {"package": "lightgbm>=4.3.0,<5.0.0"}},
    {"pypi": {"package": "mlflow>=2.11.0,<3.0.0"}},
    {"pypi": {"package": "pandas>=2.2.0,<3.0.0"}},
    {"pypi": {"package": "numpy>=1.26.0,<3.0.0"}},
    {"pypi": {"package": "snowflake-connector-python>=3.7.0,<4.0.0"}},
)

DEFAULT_ETL_LIBRARIES: tuple[dict[str, dict[str, str]], ...] = (
    {"pypi": {"package": "pydantic>=2.5.0,<3.0.0"}},
    {"pypi": {"package": "snowflake-connector-python>=3.7.0,<4.0.0"}},
)


def get_databricks_scripts_base_path() -> str:
    """Resolve Databricks script root path (UC Volume / Repos) from environment."""
    return os.environ.get("DATABRICKS_SCRIPTS_BASE_PATH", DEFAULT_DATABRICKS_SCRIPTS_BASE_PATH).rstrip("/")


def get_databricks_raw_base_path() -> str:
    """Resolve Databricks raw landing root path (UC Volume / mount) from environment."""
    return os.environ.get("DATABRICKS_RAW_BASE_PATH", DEFAULT_DATABRICKS_RAW_BASE_PATH).rstrip("/")


def build_databricks_submit_kwargs(
    script_relative_path: str,
    *,
    is_ml: bool = False,
    num_workers: int = 1,
    parameters: Optional[list[str]] = None,
    timeout_seconds: int = 1800,
) -> dict[str, Any]:
    """Build consistent DatabricksSubmitRunOperator kwargs without legacy dbfs:/FileStore paths."""
    scripts_root = get_databricks_scripts_base_path()
    python_file = f"{scripts_root}/{script_relative_path.lstrip('/')}"
    libraries = list(DEFAULT_ML_LIBRARIES if is_ml else DEFAULT_ETL_LIBRARIES)

    spark_env_vars: dict[str, str] = {"DATABRICKS_RAW_BASE_PATH": get_databricks_raw_base_path()}
    if is_ml:
        spark_env_vars["MLFLOW_TRACKING_URI"] = "databricks"

    return {
        "databricks_conn_id": "databricks_default",
        "new_cluster": {
            "spark_version": os.environ.get("DATABRICKS_SPARK_VERSION", DEFAULT_DATABRICKS_SPARK_VERSION),
            "node_type_id": os.environ.get("DATABRICKS_NODE_TYPE", DEFAULT_DATABRICKS_NODE_TYPE),
            "num_workers": num_workers,
            "spark_env_vars": spark_env_vars,
        },
        "libraries": libraries,
        "spark_python_task": {
            "python_file": python_file,
            "parameters": parameters or ["{{ ds }}"],
        },
        "timeout_seconds": timeout_seconds,
        "databricks_retry_limit": 2,
    }


def notify_task_failure(context: Mapping[str, Any]) -> None:
    """Structured failure callback logging DAG, task, execution date, and exception context."""
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", context.get("dag", "unknown"))
    task_id = getattr(task_instance, "task_id", "unknown")
    logical_date = context.get("logical_date", "unknown")
    exception = context.get("exception")
    log_url = getattr(task_instance, "log_url", "")

    logger.error(
        "AIRFLOW_TASK_FAILURE dag_id=%s task_id=%s logical_date=%s exception=%r log_url=%s",
        dag_id,
        task_id,
        logical_date,
        exception,
        log_url,
    )
