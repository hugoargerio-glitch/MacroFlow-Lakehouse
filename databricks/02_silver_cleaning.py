"""Databricks Silver cleaning job reading incrementally from Bronze Delta tables."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession


def main(argv: Optional[list[str]] = None) -> None:
    """Clean, conform, deterministically deduplicate, and merge Bronze records into Silver."""
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.transforms.silver import run_silver_cleaning

    _ = sys.argv[1:] if argv is None else argv
    incremental = os.environ.get("MACROFLOW_SILVER_FULL_REFRESH", "false").lower() != "true"

    spark = (
        SparkSession.builder.appName("MacroFlow-Silver-Cleaning")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    run_silver_cleaning(spark, incremental=incremental)


if __name__ == "__main__":
    main()
