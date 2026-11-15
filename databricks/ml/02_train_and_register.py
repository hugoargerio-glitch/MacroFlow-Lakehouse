"""Databricks Direct Multi-Horizon LightGBM training, walk-forward CV, and MLflow Alias registration."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Mapping, Optional

import mlflow
import mlflow.pyfunc
import pandas as pd
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ml_utils.features import FEATURE_COLUMNS, TARGET_COLUMNS  # noqa: E402
from src.ml_utils.training import (  # noqa: E402
    DEFAULT_LGBM_PARAMS,
    MODEL_REGISTRY_NAME,
    evaluate_direct_bundle,
    make_train_val_test_split,
    run_walk_forward_cv,
    should_promote_challenger,
)

logger = logging.getLogger("MacroFlow-Model-Training")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FEATURE_TABLE_NAME = "macroflow_gold.feature_store_fx"
EXPERIMENT_NAME = "/Shared/macroflow_usdbrl_forecaster"


def _fetch_champion_metrics(client: MlflowClient, model_name: str) -> Optional[Mapping[str, float]]:
    """Retrieve logged metrics from the current ``@champion`` alias, or None if unset."""
    try:
        champion_mv = client.get_model_version_by_alias(model_name, "champion")
        champion_run = client.get_run(champion_mv.run_id)
        return dict(champion_run.data.metrics)
    except MlflowException:
        return None


def run_training_pipeline(spark_session: SparkSession) -> dict[str, Any]:
    """Train direct h=1..7 models, run walk-forward CV, log to MLflow, and gate @champion promotion."""
    logger.info("Loading Feature Store table: %s", FEATURE_TABLE_NAME)
    df_spark = spark_session.read.table(FEATURE_TABLE_NAME).filter(F.col("ticker") == "USDBRL=X")

    # Keep only rows where all multi-horizon stationary log-return targets are available
    for target_col in TARGET_COLUMNS:
        df_spark = df_spark.filter(F.col(target_col).isNotNull())

    df_data = df_spark.toPandas()
    df_data["reference_date"] = pd.to_datetime(df_data["reference_date"])
    df_data = df_data.sort_values("reference_date").reset_index(drop=True)

    # Strict 3-way temporal split: train -> val (early stopping & CQR calibration) -> untouched test
    split = make_train_val_test_split(n_samples=len(df_data), val_size=30, test_size=60, min_train_size=120)

    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()

    with mlflow.start_run(run_name="lgbm_direct_multihorizon_fx") as run:
        mlflow.log_params(
            {
                **DEFAULT_LGBM_PARAMS,
                "forecast_strategy": "direct_multi_horizon",
                "target_type": "log_return",
                "horizons": "1..7",
                "val_size": 30,
                "test_size": 60,
                "n_features": len(FEATURE_COLUMNS),
                "n_samples": len(df_data),
            }
        )

        # 1. Walk-forward expanding-window CV across historical regimes
        cv_metrics = run_walk_forward_cv(
            df_data=df_data,
            n_splits=4,
            val_size=20,
            test_size=20,
            min_train_size=120,
            feature_columns=FEATURE_COLUMNS,
        )
        mlflow.log_metrics(cv_metrics)

        # 2. Final Direct Multi-Horizon Bundle (train/val -> holdout test)
        bundle, holdout_metrics = evaluate_direct_bundle(
            df_data=df_data,
            split=split,
            feature_columns=FEATURE_COLUMNS,
        )
        mlflow.log_metrics(holdout_metrics)

        # 3. Log MLflow PyFunc bundle with explicit signature and input_example
        input_example = df_data.loc[:, list(FEATURE_COLUMNS)].iloc[:5].astype(float)
        output_example = bundle.predict(None, input_example)
        signature = infer_signature(input_example, output_example)

        src_path = _REPO_ROOT / "src"
        model_info = mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=bundle,
            signature=signature,
            input_example=input_example,
            code_paths=[str(src_path)] if src_path.exists() else None,
            registered_model_name=MODEL_REGISTRY_NAME,
        )

        new_version = str(model_info.registered_model_version)
        client.set_registered_model_alias(MODEL_REGISTRY_NAME, "challenger", new_version)
        mlflow.set_tag("challenger_version", new_version)

        # 4. Champion-vs-Challenger & Naive Baseline Quality Gate
        champion_metrics = _fetch_champion_metrics(client, MODEL_REGISTRY_NAME)
        promote, reason = should_promote_challenger(
            challenger_metrics=holdout_metrics,
            champion_metrics=champion_metrics,
            metric_key="mean_rmse",
            naive_key="mean_naive_rmse",
        )
        mlflow.set_tag("promotion_decision", str(promote))
        mlflow.set_tag("promotion_reason", reason)
        logger.info("Promotion evaluation for v%s: promote=%s (%s)", new_version, promote, reason)

        if promote:
            client.set_registered_model_alias(MODEL_REGISTRY_NAME, "champion", new_version)
            logger.info("Promoted %s v%s to @champion alias.", MODEL_REGISTRY_NAME, new_version)

        return {
            "run_id": run.info.run_id,
            "version": new_version,
            "promoted": promote,
            "reason": reason,
            "holdout_metrics": holdout_metrics,
            "cv_metrics": cv_metrics,
        }


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("MacroFlow-Model-Training").getOrCreate()
    run_training_pipeline(spark_session)
