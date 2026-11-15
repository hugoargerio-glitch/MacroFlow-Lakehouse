"""MacroFlow Lakehouse - Serving Layer Synchronization (Databricks Gold -> Snowflake).

Reads conformed analytical market records from Delta Lake `macroflow_gold.fact_market_daily`,
maps business tickers to surrogate dimension keys (`DIM_ASSET.asset_id`), validates and casts
types, and performs an idempotent atomic `MERGE` into `MACROFLOW_DW.GOLD.FACT_MARKET_DAILY`.
"""

import logging
import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("MacroFlow-Snowflake-Sync")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Definicao cadastral dos ativos suportados para resolucao de chaves dimensionais
DIM_ASSET_MAPPING = [
    ("USDBRL=X", 1),
    ("^BVSP", 2),
    ("BTC-BRL", 3),
    ("USD_PTAX", 4),
    ("SELIC", 5),
    ("IPCA", 6),
    ("^IRX", 7),
]

ASSET_MAP_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), False),
        StructField("asset_id", IntegerType(), False),
    ]
)


def get_secret(scope: str, key: str, fallback_env: str = "") -> str:
    """Recupera segredos de forma segura via dbutils (Databricks) ou variaveis de ambiente."""
    try:
        # Tenta dbutils no ambiente Databricks Runtime
        dbutils_obj = globals().get("dbutils")
        if dbutils_obj is not None:
            return str(dbutils_obj.secrets.get(scope=scope, key=key))
        return os.environ.get(fallback_env, "")
    except Exception:  # noqa: BLE001
        return os.environ.get(fallback_env, "")


def build_snowflake_options() -> dict[str, str]:
    """Monta o dicionario de configuracoes seguras do Snowflake Spark Connector."""
    account_or_url = get_secret("macroflow_scope", "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ACCOUNT")
    explicit_url = get_secret("macroflow_scope", "SNOWFLAKE_URL", "SNOWFLAKE_URL")
    user = get_secret("macroflow_scope", "SNOWFLAKE_USER", "SNOWFLAKE_USER")
    password = get_secret("macroflow_scope", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_PASSWORD")

    if explicit_url:
        sf_url = explicit_url
    elif account_or_url:
        sf_url = (
            account_or_url if account_or_url.startswith("http") else f"https://{account_or_url}.snowflakecomputing.com"
        )
    else:
        sf_url = ""

    if not sf_url or "<your_account_identifier>" in sf_url:
        raise ValueError("SNOWFLAKE_ACCOUNT or SNOWFLAKE_URL secret/env var must be configured.")
    if not user or not password:
        raise ValueError("SNOWFLAKE_USER and SNOWFLAKE_PASSWORD secrets/env vars must be configured.")

    return {
        "sfURL": sf_url,
        "sfUser": user,
        "sfPassword": password,
        "sfDatabase": "MACROFLOW_DW",
        "sfSchema": "GOLD",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "MACROFLOW_ETL_ROLE",
    }


def prepare_gold_for_snowflake(df_gold: DataFrame, df_asset_map: DataFrame) -> DataFrame:
    """Normaliza colunas, resolve surrogate keys e garante tipagem compativel com o DDL da FACT_MARKET_DAILY."""
    # 1. Resolucao da Chave Substituta da Dimensao (ticker -> asset_id)
    df_joined = df_gold.join(df_asset_map, on="ticker", how="inner")

    # 2. Tratamento defensivo de colunas opcionais ou calculadas
    has_spread = "selic_vs_us_spread" in df_gold.columns
    spread_col = (
        F.col("selic_vs_us_spread").cast(DecimalType(10, 4)) if has_spread else F.lit(None).cast(DecimalType(10, 4))
    )

    has_timestamp = "calculated_at" in df_gold.columns
    timestamp_col = F.col("calculated_at").cast(TimestampType()) if has_timestamp else F.current_timestamp()

    # 3. Projecao com tipagem estrita alinhada ao schema Snowflake
    df_prepared = df_joined.select(
        F.col("reference_date").cast(DateType()).alias("REFERENCE_DATE"),
        F.col("asset_id").cast(IntegerType()).alias("ASSET_ID"),
        F.col("close_price").cast(DecimalType(18, 8)).alias("CLOSE_PRICE"),
        F.col("daily_return").cast(DecimalType(12, 6)).alias("DAILY_RETURN"),
        F.col("log_return").cast(DecimalType(12, 6)).alias("LOG_RETURN"),
        F.col("sma_7").cast(DecimalType(18, 8)).alias("SMA_7"),
        F.col("sma_30").cast(DecimalType(18, 8)).alias("SMA_30"),
        F.col("sma_200").cast(DecimalType(18, 8)).alias("SMA_200"),
        F.col("rolling_vol_30d").cast(DecimalType(12, 6)).alias("ROLLING_VOL_30D"),
        F.col("rolling_vol_60d").cast(DecimalType(12, 6)).alias("ROLLING_VOL_60D"),
        F.col("rolling_vol_90d").cast(DecimalType(12, 6)).alias("ROLLING_VOL_90D"),
        F.col("real_interest_rate").cast(DecimalType(10, 4)).alias("REAL_INTEREST_RATE"),
        spread_col.alias("SELIC_VS_US_SPREAD"),
        timestamp_col.alias("INGESTION_TIMESTAMP"),
    )

    return df_prepared


def generate_merge_sql(target_table: str, staging_table: str) -> str:
    """Gera query transacional de MERGE para sincronizacao idempotente entre a stage e o fato definitivo."""
    return f"""
    MERGE INTO {target_table} AS target
    USING {staging_table} AS source
    ON target.reference_date = source.reference_date
       AND target.asset_id = source.asset_id
    WHEN MATCHED THEN UPDATE SET
        target.close_price = source.close_price,
        target.daily_return = source.daily_return,
        target.log_return = source.log_return,
        target.sma_7 = source.sma_7,
        target.sma_30 = source.sma_30,
        target.sma_200 = source.sma_200,
        target.rolling_vol_30d = source.rolling_vol_30d,
        target.rolling_vol_60d = source.rolling_vol_60d,
        target.rolling_vol_90d = source.rolling_vol_90d,
        target.real_interest_rate = source.real_interest_rate,
        target.selic_vs_us_spread = source.selic_vs_us_spread,
        target.ingestion_timestamp = source.ingestion_timestamp
    WHEN NOT MATCHED THEN INSERT (
        reference_date,
        asset_id,
        close_price,
        daily_return,
        log_return,
        sma_7,
        sma_30,
        sma_200,
        rolling_vol_30d,
        rolling_vol_60d,
        rolling_vol_90d,
        real_interest_rate,
        selic_vs_us_spread,
        ingestion_timestamp
    ) VALUES (
        source.reference_date,
        source.asset_id,
        source.close_price,
        source.daily_return,
        source.log_return,
        source.sma_7,
        source.sma_30,
        source.sma_200,
        source.rolling_vol_30d,
        source.rolling_vol_60d,
        source.rolling_vol_90d,
        source.real_interest_rate,
        source.selic_vs_us_spread,
        source.ingestion_timestamp
    );
    TRUNCATE TABLE IF EXISTS {staging_table};
    """


def sync_gold_to_snowflake(
    spark: SparkSession,
    gold_table_name: str = "macroflow_gold.fact_market_daily",
    target_dw_table: str = "MACROFLOW_DW.GOLD.FACT_MARKET_DAILY",
    staging_dw_table: str = "MACROFLOW_DW.GOLD.STG_FACT_MARKET_DAILY",
) -> None:
    """Executa o pipeline completo de sincronizacao Gold -> Snowflake."""
    logger.info("Iniciando leitura da tabela Gold Delta: %s", gold_table_name)
    df_gold = spark.read.table(gold_table_name)

    df_asset_map = spark.createDataFrame(DIM_ASSET_MAPPING, schema=ASSET_MAP_SCHEMA)
    df_prepared = prepare_gold_for_snowflake(df_gold, df_asset_map)

    snowflake_options = build_snowflake_options()
    merge_sql = generate_merge_sql(target_dw_table, staging_dw_table)

    logger.info(
        "Carregando dados preparados na tabela de staging do Snowflake: %s",
        staging_dw_table,
    )
    (
        df_prepared.write.format("snowflake")
        .options(**snowflake_options)
        .option("dbtable", staging_dw_table)
        .option("column_mapping", "name")
        .option("usestagingtable", "ON")
        .option("postactions", merge_sql)
        .mode("overwrite")
        .save()
    )
    logger.info("Sincronizacao idempotente no Snowflake finalizada com sucesso via MERGE.")


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder.appName("MacroFlow-Snowflake-Sync")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    sync_gold_to_snowflake(spark_session)
