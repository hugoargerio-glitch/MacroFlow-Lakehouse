from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    "owner": "macroflow-mlops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="macroflow_ml_inference_batch",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Disparada via Trigger pela DAG de Lakehouse
    catchup=False,
    tags=["mlops", "lightgbm", "forecasting", "snowflake", "batch"]
) as dag:

    run_batch_scoring = DatabricksSubmitRunOperator(
        task_id="execute_lgbm_batch_scoring",
        databricks_conn_id="databricks_default",
        new_cluster={
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 1,
            "spark_env_vars": {
                "MLFLOW_TRACKING_URI": "databricks"
            }
        },
        spark_python_task={
            "python_file": "dbfs:/FileStore/scripts/ml/03_batch_predict.py"
        }
    )

    run_batch_scoring
