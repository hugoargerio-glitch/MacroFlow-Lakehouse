"""Databricks batch inference pipeline loading @champion direct FX models and merging to Snowflake."""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import mlflow.pyfunc
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ml_utils.features import FEATURE_COLUMNS  # noqa: E402
from src.ml_utils.inference import (  # noqa: E402
    USDBRL_ASSET_ID,
    build_forecast_records,
    build_snowflake_options,
    generate_forecast_merge_sql,
    get_forecast_spark_schema,
    validate_feature_freshness,
)
from src.ml_utils.training import MODEL_REGISTRY_NAME  # noqa: E402

logger = logging.getLogger("MacroFlow-Batch-Inference")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

FEATURE_TABLE_NAME = "macroflow_gold.feature_store_fx"
TARGET_DW_TABLE = "MACROFLOW_DW.GOLD.FACT_MACRO_FORECASTS"
STAGING_DW_TABLE = "MACROFLOW_DW.GOLD.STG_FACT_MACRO_FORECASTS"


def _resolve_as_of_date(cli_arg: Optional[str] = None) -> date:
    if cli_arg:
        return datetime.strptime(cli_arg.strip()[:10], "%Y-%m-%d").date()
    return date.today()


def run_batch_inference(spark_session: SparkSession, as_of_date: Optional[date] = None) -> int:
    """Score t+1..t+7 B3 trading days via @champion model and merge idempotently into Snowflake."""
    eval_date = as_of_date or date.today()
    max_staleness = int(os.environ.get("MACROFLOW_MAX_FEATURE_STALENESS_DAYS", "5"))

    # 1. Atomic resolution of @champion model version via MLflow Aliases
    client = MlflowClient()
    champion_mv = client.get_model_version_by_alias(MODEL_REGISTRY_NAME, "champion")
    champion_version = str(champion_mv.version)
    model_uri = f"models:/{MODEL_REGISTRY_NAME}/{champion_version}"
    logger.info("Loading @champion model URI: %s", model_uri)
    model = mlflow.pyfunc.load_model(model_uri)

    # 2. Fetch latest USDBRL=X Feature Store observation and enforce freshness
    df_latest = (
        spark_session.read.table(FEATURE_TABLE_NAME)
        .filter(F.col("ticker") == "USDBRL=X")
        .sort(F.col("reference_date").desc())
        .limit(1)
        .toPandas()
    )
    if df_latest.empty:
        raise ValueError("Feature Store is empty for USDBRL=X; cannot run batch inference.")

    ref_date = pd.to_datetime(df_latest["reference_date"].iloc[0]).date()
    validate_feature_freshness(reference_date=ref_date, as_of_date=eval_date, max_staleness_days=max_staleness)

    current_price = float(df_latest["close_price"].iloc[0])
    x_latest = df_latest.loc[:, list(FEATURE_COLUMNS)].astype(float)
    if x_latest.isna().any().any():
        nan_cols = x_latest.columns[x_latest.isna().iloc[0]].tolist()
        raise ValueError(f"Latest Feature Store row ({ref_date}) contains NaNs in columns: {nan_cols}")

    # 3. Direct multi-horizon log-return prediction (h=1..7) & B3 trading-day mapping
    predictions_df = model.predict(x_latest)
    model_version_tag = f"LightGBM_Direct_v{champion_version}"
    forecast_records = build_forecast_records(
        reference_date=ref_date,
        current_price=current_price,
        predictions_df=predictions_df,
        model_version=model_version_tag,
        asset_id=USDBRL_ASSET_ID,
    )

    # 4. Idempotent staged MERGE into Snowflake FACT_MACRO_FORECASTS
    forecast_schema = get_forecast_spark_schema()
    df_forecast_spark = spark_session.createDataFrame(forecast_records, schema=forecast_schema)

    dbutils_obj = globals().get("dbutils")
    snowflake_options = build_snowflake_options(dbutils_obj=dbutils_obj)
    merge_sql = generate_forecast_merge_sql(target_table=TARGET_DW_TABLE, staging_table=STAGING_DW_TABLE)

    (
        df_forecast_spark.write.format("snowflake")
        .options(**snowflake_options)
        .option("dbtable", STAGING_DW_TABLE)
        .option("column_mapping", "name")
        .option("usestagingtable", "ON")
        .option("postactions", merge_sql)
        .mode("overwrite")
        .save()
    )

    logger.info(
        "Merged %d forecast horizons for reference_date=%s (version=%s) into %s.",
        len(forecast_records),
        ref_date,
        model_version_tag,
        TARGET_DW_TABLE,
    )
    return len(forecast_records)


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("MacroFlow-Batch-Inference").getOrCreate()
    cli_date = _resolve_as_of_date(sys.argv[1] if len(sys.argv) > 1 else None)
    run_batch_inference(spark_session, as_of_date=cli_date)
