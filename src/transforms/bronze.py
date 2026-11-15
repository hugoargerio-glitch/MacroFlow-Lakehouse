"""Pure PySpark transformations and Delta Lake writers for MacroFlow Bronze layer."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Optional

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.utils import AnalysisException

from src.extractors.airflow_helpers import get_databricks_raw_base_path
from src.schemas.macro_models import get_bcb_spark_schema, get_market_spark_schema
from src.transforms._delta import create_clustered_delta_table, ensure_schema_exists

logger = logging.getLogger(__name__)

DEFAULT_BRONZE_BCB_TABLE = "macroflow_bronze.raw_bcb"
DEFAULT_BRONZE_YFINANCE_TABLE = "macroflow_bronze.raw_yfinance"


def coerce_target_date(target_date: Optional[date | datetime | str]) -> Optional[date]:
    """Normalize an optional CLI/Airflow date argument into a ``datetime.date``."""
    if target_date is None:
        return None
    if isinstance(target_date, datetime):
        return target_date.date()
    if isinstance(target_date, date):
        return target_date
    cleaned = str(target_date).strip()
    if not cleaned:
        return None
    return date.fromisoformat(cleaned[:10])


def build_partition_subpath(target_date: date | datetime | str) -> str:
    """Return the Hive-style ``year=YYYY/month=MM/day=DD`` relative path for a date."""
    coerced = coerce_target_date(target_date)
    if coerced is None:
        raise ValueError("target_date cannot be None when building a partition subpath.")
    return f"year={coerced:%Y}/month={coerced:%m}/day={coerced:%d}"


def resolve_raw_source_path(
    *,
    source: str,
    raw_base_path: Optional[str] = None,
    target_date: Optional[date | datetime | str] = None,
) -> tuple[str, str]:
    """Resolve ``(read_path, hive_base_path)`` for a raw edge source (``bcb`` or ``yfinance``)."""
    root = (raw_base_path or get_databricks_raw_base_path()).rstrip("/")
    hive_base_path = f"{root}/{source.strip('/')}"
    coerced = coerce_target_date(target_date)
    if coerced is None:
        return hive_base_path, hive_base_path
    return f"{hive_base_path}/{build_partition_subpath(coerced)}", hive_base_path


def build_bronze_schema(payload_schema: StructType) -> StructType:
    """Extend a raw payload StructType with Hive partition and Bronze audit metadata fields."""
    existing_names = {field.name for field in payload_schema.fields}
    extra_fields = [
        StructField("year", IntegerType(), nullable=True),
        StructField("month", IntegerType(), nullable=True),
        StructField("day", IntegerType(), nullable=True),
        StructField("source_partition_date", DateType(), nullable=False),
        StructField("source_file", StringType(), nullable=False),
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
    ]
    return StructType(
        list(payload_schema.fields) + [field for field in extra_fields if field.name not in existing_names]
    )


def add_bronze_metadata(
    df: DataFrame,
    *,
    target_date: Optional[date | datetime | str] = None,
    ingestion_ts: Optional[datetime] = None,
) -> DataFrame:
    """Attach retained Hive partition columns, ``source_partition_date``, ``source_file``, and timestamp."""
    coerced = coerce_target_date(target_date)
    has_hive_cols = all(col_name in df.columns for col_name in ("year", "month", "day"))

    if has_hive_cols:
        enriched = (
            df.withColumn("year", F.col("year").cast(IntegerType()))
            .withColumn("month", F.col("month").cast(IntegerType()))
            .withColumn("day", F.col("day").cast(IntegerType()))
            .withColumn(
                "source_partition_date",
                F.make_date(F.col("year"), F.col("month"), F.col("day")),
            )
        )
    else:
        fallback_date = F.lit(coerced.isoformat()).cast(DateType()) if coerced else F.current_date()
        enriched = (
            df.withColumn("source_partition_date", fallback_date)
            .withColumn("year", F.year(F.col("source_partition_date")).cast(IntegerType()))
            .withColumn("month", F.month(F.col("source_partition_date")).cast(IntegerType()))
            .withColumn("day", F.dayofmonth(F.col("source_partition_date")).cast(IntegerType()))
        )

    ts_expr = F.lit(ingestion_ts).cast(TimestampType()) if ingestion_ts is not None else F.current_timestamp()
    return enriched.withColumn("source_file", F.input_file_name()).withColumn("ingestion_timestamp", ts_expr)


def read_raw_ndjson_batch(
    spark: SparkSession,
    *,
    source: str,
    schema: StructType,
    raw_base_path: Optional[str] = None,
    target_date: Optional[date | datetime | str] = None,
    ingestion_ts: Optional[datetime] = None,
) -> DataFrame:
    """Read strict NDJSON files from edge storage using explicit schema and Hive ``basePath``."""
    read_path, hive_base_path = resolve_raw_source_path(
        source=source,
        raw_base_path=raw_base_path,
        target_date=target_date,
    )

    try:
        df_raw = (
            spark.read.schema(schema)
            .option("multiLine", "false")
            .option("mode", "FAILFAST")
            .option("dateFormat", "yyyy-MM-dd")
            .option("basePath", hive_base_path)
            .json(read_path)
        )
    except AnalysisException as exc:
        message = str(exc)
        if "PATH_NOT_FOUND" in message or "Path does not exist" in message:
            logger.warning("Raw source path not found (%s); returning empty Bronze DataFrame.", read_path)
            return spark.createDataFrame([], schema=build_bronze_schema(schema))
        raise

    return add_bronze_metadata(df_raw, target_date=target_date, ingestion_ts=ingestion_ts)


def deduplicate_against_existing_files(
    df_bronze: DataFrame,
    df_existing_files: Optional[DataFrame],
) -> DataFrame:
    """Exclude records whose ``source_file`` has already been ingested into Bronze."""
    if df_existing_files is None:
        return df_bronze
    return df_bronze.join(df_existing_files.select("source_file").distinct(), on="source_file", how="left_anti")


def write_bronze_delta(
    spark: SparkSession,
    df_bronze: DataFrame,
    table_name: str,
    *,
    target_date: Optional[date | datetime | str] = None,
) -> None:
    """Write Bronze records idempotently into Delta using partition-scoped ``replaceWhere`` or file anti-join."""
    coerced = coerce_target_date(target_date)

    if not spark.catalog.tableExists(table_name):
        create_clustered_delta_table(
            spark,
            df_bronze,
            table_name,
            ("source_partition_date",),
            logger=logger,
            merge_schema=True,
        )
        return

    if coerced is not None:
        replace_predicate = f"source_partition_date = '{coerced.isoformat()}'"
        if df_bronze.rdd.isEmpty():
            DeltaTable.forName(spark, table_name).delete(replace_predicate)
            return
        (
            df_bronze.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_predicate)
            .saveAsTable(table_name)
        )
        return

    existing_files = spark.read.table(table_name).select("source_file").distinct()
    df_new_files = deduplicate_against_existing_files(df_bronze, existing_files)
    if not df_new_files.rdd.isEmpty():
        df_new_files.write.format("delta").mode("append").saveAsTable(table_name)


def run_bronze_autoloader(
    spark: SparkSession,
    *,
    source: str,
    schema: StructType,
    table_name: str,
    raw_base_path: Optional[str] = None,
    checkpoint_base_path: Optional[str] = None,
) -> None:
    """Ingest raw NDJSON incrementally via Databricks Auto Loader (``cloudFiles``)."""
    hive_base_path, _ = resolve_raw_source_path(source=source, raw_base_path=raw_base_path, target_date=None)
    ckpt_root = (
        checkpoint_base_path
        or os.environ.get("DATABRICKS_BRONZE_CHECKPOINT_PATH", "/Volumes/macroflow/bronze/_checkpoints")
    ).rstrip("/")
    checkpoint_location = f"{ckpt_root}/{source}"
    ensure_schema_exists(spark, table_name)

    df_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_location}/schema")
        .option("multiLine", "false")
        .option("dateFormat", "yyyy-MM-dd")
        .option("basePath", hive_base_path)
        .schema(schema)
        .load(hive_base_path)
    )
    df_enriched = add_bronze_metadata(df_stream)
    query = (
        df_enriched.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(table_name)
    )
    query.awaitTermination()


def ingest_bronze_sources(
    spark: SparkSession,
    *,
    raw_base_path: Optional[str] = None,
    target_date: Optional[date | datetime | str] = None,
    bcb_table: str = DEFAULT_BRONZE_BCB_TABLE,
    yfinance_table: str = DEFAULT_BRONZE_YFINANCE_TABLE,
    use_autoloader: Optional[bool] = None,
) -> tuple[DataFrame, DataFrame]:
    """Execute Bronze ingestion for both BCB SGS and Yahoo Finance NDJSON feeds."""
    autoloader_enabled = (
        use_autoloader
        if use_autoloader is not None
        else os.environ.get("DATABRICKS_USE_AUTOLOADER", "false").lower() == "true"
    )
    bcb_schema = get_bcb_spark_schema()
    market_schema = get_market_spark_schema()

    if autoloader_enabled and coerce_target_date(target_date) is None:
        run_bronze_autoloader(
            spark,
            source="bcb",
            schema=bcb_schema,
            table_name=bcb_table,
            raw_base_path=raw_base_path,
        )
        run_bronze_autoloader(
            spark,
            source="yfinance",
            schema=market_schema,
            table_name=yfinance_table,
            raw_base_path=raw_base_path,
        )
        return spark.read.table(bcb_table), spark.read.table(yfinance_table)

    df_bcb_bronze = read_raw_ndjson_batch(
        spark,
        source="bcb",
        schema=bcb_schema,
        raw_base_path=raw_base_path,
        target_date=target_date,
    )
    write_bronze_delta(spark, df_bcb_bronze, bcb_table, target_date=target_date)

    df_yf_bronze = read_raw_ndjson_batch(
        spark,
        source="yfinance",
        schema=market_schema,
        raw_base_path=raw_base_path,
        target_date=target_date,
    )
    write_bronze_delta(spark, df_yf_bronze, yfinance_table, target_date=target_date)

    return df_bcb_bronze, df_yf_bronze
