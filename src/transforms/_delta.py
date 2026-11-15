"""Shared Delta Lake table creation helpers with Liquid Clustering fallback."""

import logging

from pyspark.sql import DataFrame, SparkSession


def ensure_schema_exists(spark: SparkSession, table_name: str) -> None:
    """Create the target Spark catalog schema when ``table_name`` is schema-qualified."""
    if "." in table_name:
        schema_name = table_name.rsplit(".", 1)[0]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def create_clustered_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    cluster_cols: tuple[str, ...],
    *,
    logger: logging.Logger,
    merge_schema: bool = False,
    zorder_fallback: bool = False,
) -> None:
    """Create a Delta table with Liquid Clustering, falling back when ``clusterBy`` is unavailable."""
    ensure_schema_exists(spark, table_name)
    try:
        writer = df.writeTo(table_name).using("delta")
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        cluster_by = getattr(writer, "clusterBy", None)
        if callable(cluster_by):
            cluster_by(*cluster_cols).createOrReplace()
            return
    except Exception:  # noqa: BLE001
        pass

    logger.info("Falling back to unpartitioned Delta table creation for %s.", table_name)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    if zorder_fallback and cluster_cols:
        cols_sql = ", ".join(cluster_cols)
        try:
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols_sql})")
        except Exception:  # noqa: BLE001
            pass
