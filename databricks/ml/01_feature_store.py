"""Databricks Feature Store pipeline for MacroFlow USD/BRL multi-horizon forecasting."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# Ensure repository root is importable when executed as a standalone Databricks Python task
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.ml_utils.features import build_fx_feature_store  # noqa: E402

logger = logging.getLogger("MacroFlow-Feature-Store")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

GOLD_TABLE_NAME = "macroflow_gold.fact_market_daily"
FEATURE_TABLE_NAME = "macroflow_gold.feature_store_fx"


def run_feature_store_pipeline(spark_session: SparkSession) -> int:
    """Build and idempotently merge the USD/BRL Feature Store table from Gold analytics."""
    logger.info("Reading Gold analytics table: %s", GOLD_TABLE_NAME)
    df_gold = spark_session.read.table(GOLD_TABLE_NAME)

    df_feature_store = build_fx_feature_store(df_gold=df_gold)

    if not spark_session.catalog.tableExists(FEATURE_TABLE_NAME):
        logger.info("Creating Feature Store Delta table: %s", FEATURE_TABLE_NAME)
        (
            df_feature_store.write.format("delta")
            .option("mergeSchema", "true")
            .clusterBy("reference_date")
            .saveAsTable(FEATURE_TABLE_NAME)
        )
    else:
        logger.info("Merging incremental Feature Store updates into: %s", FEATURE_TABLE_NAME)
        spark_session.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        delta_fs = DeltaTable.forName(spark_session, FEATURE_TABLE_NAME)
        (
            delta_fs.alias("target")
            .merge(
                source=df_feature_store.alias("source"),
                condition="target.reference_date = source.reference_date AND target.ticker = source.ticker",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    row_count = df_feature_store.count()
    logger.info("Feature Store pipeline finished successfully (%d rows).", row_count)
    return row_count


if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("MacroFlow-Feature-Store").getOrCreate()
    run_feature_store_pipeline(spark_session)
