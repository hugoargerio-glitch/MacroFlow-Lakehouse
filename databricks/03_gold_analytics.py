"""Databricks Gold analytics job computing asset metrics and macroeconomic indicators."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession


def main(argv: Optional[list[str]] = None) -> None:
    """Compute Gold asset returns/SMAs/volatilities, Fisher real rate, and SELIC-US spread."""
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.transforms.gold import run_gold_analytics

    _ = sys.argv[1:] if argv is None else argv
    lookback_env = os.environ.get("MACROFLOW_GOLD_MERGE_LOOKBACK_DAYS")
    lookback_days = int(lookback_env) if lookback_env and lookback_env.isdigit() else None

    spark = (
        SparkSession.builder.appName("MacroFlow-Gold-Analytics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    run_gold_analytics(spark, lookback_days=lookback_days)


if __name__ == "__main__":
    main()
