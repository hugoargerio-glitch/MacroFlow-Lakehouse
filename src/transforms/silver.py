"""Pure PySpark transformations and Delta Lake MERGE logic for MacroFlow Silver layer."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import DateType, DecimalType, StringType, TimestampType

from src.transforms._delta import create_clustered_delta_table
from src.transforms.bronze import DEFAULT_BRONZE_BCB_TABLE, DEFAULT_BRONZE_YFINANCE_TABLE

logger = logging.getLogger(__name__)

DEFAULT_SILVER_TABLE = "macroflow_silver.market_conformed"
SILVER_PRICE_DECIMAL = DecimalType(18, 8)
SILVER_VOLUME_DECIMAL = DecimalType(24, 4)
BCB_DATA_SOURCE = "BCB_SGS"
YFINANCE_DATA_SOURCE = "YAHOO_FINANCE"


def _finite_double_expr(col_name: str) -> F.Column:
    """Return a finite DoubleType column expression, converting NaN/Inf to NULL."""
    as_double = F.col(col_name).cast("double")
    return F.when(
        as_double.isNotNull()
        & (~F.isnan(as_double))
        & (as_double != F.lit(float("inf")))
        & (as_double != F.lit(float("-inf"))),
        as_double,
    )


def _optional_col(df: DataFrame, col_name: str, data_type: object) -> F.Column:
    if col_name in df.columns:
        return F.col(col_name).cast(data_type)  # type: ignore[arg-type]
    return F.lit(None).cast(data_type)  # type: ignore[arg-type]


def filter_incremental_bronze(df_bronze: DataFrame, watermark_ts: Optional[datetime]) -> DataFrame:
    """Filter a Bronze DataFrame to rows strictly newer than ``watermark_ts``."""
    if watermark_ts is None or "ingestion_timestamp" not in df_bronze.columns:
        return df_bronze
    return df_bronze.filter(F.col("ingestion_timestamp") > F.lit(watermark_ts).cast(TimestampType()))


def standardize_bcb_bronze(df_bcb_bronze: DataFrame) -> DataFrame:
    """Standardize BCB SGS Bronze records to the conformed Silver schema and domain bounds."""
    df_prepped = (
        df_bcb_bronze.withColumn("_ref_date", F.to_date(F.col("reference_date")))
        .withColumn("_ticker", F.trim(F.col("ticker_or_code").cast(StringType())))
        .withColumn("_close_dbl", _finite_double_expr("close_price"))
    )

    domain_valid = (
        F.col("_ref_date").isNotNull()
        & F.col("_ticker").isNotNull()
        & (F.length(F.col("_ticker")) > 0)
        & F.col("_close_dbl").isNotNull()
        & (
            ((F.col("_ticker") == "USD_PTAX") & (F.col("_close_dbl") > 0.0))
            | ((F.col("_ticker") == "SELIC") & (F.col("_close_dbl") >= 0.0))
            | ((F.col("_ticker") == "IPCA") & (F.col("_close_dbl") > -100.0))
            | (~F.col("_ticker").isin("USD_PTAX", "SELIC", "IPCA"))
        )
    )

    return df_prepped.filter(domain_valid).select(
        F.col("_ref_date").alias("reference_date"),
        F.col("_ticker").alias("ticker"),
        F.lit(None).cast(SILVER_PRICE_DECIMAL).alias("open_price"),
        F.lit(None).cast(SILVER_PRICE_DECIMAL).alias("high_price"),
        F.lit(None).cast(SILVER_PRICE_DECIMAL).alias("low_price"),
        F.col("_close_dbl").cast(SILVER_PRICE_DECIMAL).alias("close_price"),
        F.lit(0).cast(SILVER_VOLUME_DECIMAL).alias("volume"),
        F.lit(BCB_DATA_SOURCE).alias("data_source"),
        _optional_col(df_bcb_bronze, "ingestion_timestamp", TimestampType()).alias("bronze_ingestion_timestamp"),
        _optional_col(df_bcb_bronze, "source_partition_date", DateType()).alias("source_partition_date"),
        _optional_col(df_bcb_bronze, "source_file", StringType()).alias("source_file"),
    )


def standardize_yfinance_bronze(df_yf_bronze: DataFrame) -> DataFrame:
    """Standardize Yahoo Finance Bronze records to the conformed Silver schema and domain bounds."""
    df_prepped = (
        df_yf_bronze.withColumn("_ref_date", F.to_date(F.col("reference_date")))
        .withColumn("_ticker", F.trim(F.col("ticker").cast(StringType())))
        .withColumn("_open_dbl", _finite_double_expr("open_price"))
        .withColumn("_high_dbl", _finite_double_expr("high_price"))
        .withColumn("_low_dbl", _finite_double_expr("low_price"))
        .withColumn("_close_dbl", _finite_double_expr("close_price"))
        .withColumn("_volume_dbl", _finite_double_expr("volume"))
    )

    domain_valid = (
        F.col("_ref_date").isNotNull()
        & F.col("_ticker").isNotNull()
        & (F.length(F.col("_ticker")) > 0)
        & F.col("_close_dbl").isNotNull()
        & (F.col("_close_dbl") > 0.0)
    )

    return df_prepped.filter(domain_valid).select(
        F.col("_ref_date").alias("reference_date"),
        F.col("_ticker").alias("ticker"),
        F.when(F.col("_open_dbl") > 0.0, F.col("_open_dbl")).cast(SILVER_PRICE_DECIMAL).alias("open_price"),
        F.when(F.col("_high_dbl") > 0.0, F.col("_high_dbl")).cast(SILVER_PRICE_DECIMAL).alias("high_price"),
        F.when(F.col("_low_dbl") > 0.0, F.col("_low_dbl")).cast(SILVER_PRICE_DECIMAL).alias("low_price"),
        F.col("_close_dbl").cast(SILVER_PRICE_DECIMAL).alias("close_price"),
        F.when(F.col("_volume_dbl") >= 0.0, F.col("_volume_dbl"))
        .otherwise(F.lit(0.0))
        .cast(SILVER_VOLUME_DECIMAL)
        .alias("volume"),
        F.lit(YFINANCE_DATA_SOURCE).alias("data_source"),
        _optional_col(df_yf_bronze, "ingestion_timestamp", TimestampType()).alias("bronze_ingestion_timestamp"),
        _optional_col(df_yf_bronze, "source_partition_date", DateType()).alias("source_partition_date"),
        _optional_col(df_yf_bronze, "source_file", StringType()).alias("source_file"),
    )


def deduplicate_silver_records(
    df_union: DataFrame,
    *,
    silver_ingestion_ts: Optional[datetime] = None,
) -> DataFrame:
    """Deterministically deduplicate by ``(reference_date, ticker)`` keeping the newest ingested record."""
    w_dedup = Window.partitionBy("reference_date", "ticker").orderBy(
        F.col("bronze_ingestion_timestamp").desc_nulls_last(),
        F.col("source_partition_date").desc_nulls_last(),
        F.col("source_file").desc_nulls_last(),
        F.col("close_price").desc_nulls_last(),
        F.col("volume").desc_nulls_last(),
    )

    ts_expr = (
        F.lit(silver_ingestion_ts).cast(TimestampType()) if silver_ingestion_ts is not None else F.current_timestamp()
    )

    return (
        df_union.withColumn("_row_num", F.row_number().over(w_dedup))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "source_partition_date", "source_file")
        .withColumn("ingestion_timestamp", ts_expr)
    )


def build_silver_conformed(
    df_bcb_bronze: DataFrame,
    df_yf_bronze: DataFrame,
    *,
    silver_ingestion_ts: Optional[datetime] = None,
) -> DataFrame:
    """Conform, union, and deterministically deduplicate BCB and Yahoo Finance Bronze datasets.

    Market assets and macroeconomic series are preserved strictly on their real observed dates
    (no synthetic no-op forward-fill is applied in Silver).
    """
    df_bcb_std = standardize_bcb_bronze(df_bcb_bronze)
    df_yf_std = standardize_yfinance_bronze(df_yf_bronze)
    df_union = df_bcb_std.unionByName(df_yf_std)
    return deduplicate_silver_records(df_union, silver_ingestion_ts=silver_ingestion_ts)


def _get_source_watermarks(
    spark: SparkSession,
    target_table: str,
) -> tuple[Optional[datetime], Optional[datetime]]:
    if not spark.catalog.tableExists(target_table):
        return None, None
    df_target = spark.read.table(target_table)
    if "bronze_ingestion_timestamp" not in df_target.columns:
        return None, None

    row = df_target.agg(
        F.max(F.when(F.col("data_source") == BCB_DATA_SOURCE, F.col("bronze_ingestion_timestamp"))).alias("bcb_wm"),
        F.max(F.when(F.col("data_source") == YFINANCE_DATA_SOURCE, F.col("bronze_ingestion_timestamp"))).alias("yf_wm"),
    ).collect()[0]
    return row["bcb_wm"], row["yf_wm"]


def merge_silver_conformed(
    spark: SparkSession,
    df_silver: DataFrame,
    target_table: str = DEFAULT_SILVER_TABLE,
) -> None:
    """Persist Silver records using Liquid Clustering and a lookback, change-guarded Delta MERGE."""
    if not spark.catalog.tableExists(target_table):
        create_clustered_delta_table(
            spark,
            df_silver,
            target_table,
            ("ticker", "reference_date"),
            logger=logger,
            zorder_fallback=True,
        )
        return

    bounds = df_silver.agg(F.min("reference_date").alias("min_date")).collect()[0]
    min_ref_date = bounds["min_date"]
    if min_ref_date is None:
        logger.info("No incremental Silver records to merge into %s.", target_table)
        return

    merge_condition = (
        "target.reference_date = source.reference_date "
        "AND target.ticker = source.ticker "
        f"AND target.reference_date >= DATE '{min_ref_date.isoformat()}'"
    )
    change_predicate = (
        "NOT (target.close_price <=> source.close_price) "
        "OR NOT (target.open_price <=> source.open_price) "
        "OR NOT (target.high_price <=> source.high_price) "
        "OR NOT (target.low_price <=> source.low_price) "
        "OR NOT (target.volume <=> source.volume) "
        "OR NOT (target.data_source <=> source.data_source)"
    )

    delta_silver = DeltaTable.forName(spark, target_table)
    (
        delta_silver.alias("target")
        .merge(source=df_silver.alias("source"), condition=merge_condition)
        .whenMatchedUpdateAll(condition=change_predicate)
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver_cleaning(
    spark: SparkSession,
    *,
    bcb_table: str = DEFAULT_BRONZE_BCB_TABLE,
    yfinance_table: str = DEFAULT_BRONZE_YFINANCE_TABLE,
    target_table: str = DEFAULT_SILVER_TABLE,
    incremental: bool = True,
) -> DataFrame:
    """Read Bronze Delta tables incrementally, clean/deduplicate, and merge into Silver."""
    bcb_wm, yf_wm = _get_source_watermarks(spark, target_table) if incremental else (None, None)

    df_bcb_bronze = filter_incremental_bronze(spark.read.table(bcb_table), bcb_wm)
    df_yf_bronze = filter_incremental_bronze(spark.read.table(yfinance_table), yf_wm)

    df_silver = build_silver_conformed(df_bcb_bronze, df_yf_bronze)
    merge_silver_conformed(spark, df_silver, target_table=target_table)
    return df_silver
