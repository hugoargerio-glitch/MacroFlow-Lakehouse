"""Databricks Bronze ingestion job for BCB SGS and Yahoo Finance NDJSON feeds."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession


def main(argv: Optional[list[str]] = None) -> None:
    """Run incremental or partition-scoped Bronze Delta ingestion."""
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.transforms.bronze import ingest_bronze_sources

    args = sys.argv[1:] if argv is None else argv
    target_date = args[0] if args else os.environ.get("MACROFLOW_TARGET_DATE")

    spark = (
        SparkSession.builder.appName("MacroFlow-Bronze-Ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    ingest_bronze_sources(spark, target_date=target_date)


if __name__ == "__main__":
    main()
