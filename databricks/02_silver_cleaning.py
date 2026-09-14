import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("MacroFlow-Silver-Cleaning").getOrCreate()

# 1. Caminhos de Leitura dos JSONs Brutos (Bronze Layer)
raw_bcb_path = "/mnt/raw_data/bcb/*/*/*/*.json"
raw_yfinance_path = "/mnt/raw_data/yfinance/*/*/*/*.json"

# 2. Leitura e Padronização de Séries do BCB (SELIC, IPCA, USD_PTAX)
df_bcb_raw = spark.read.json(raw_bcb_path)

df_bcb_standardized = df_bcb_raw.select(
    F.to_date(F.col("reference_date")).alias("reference_date"),
    F.col("ticker_or_code").alias("ticker"),
    F.lit(None).cast(DecimalType(18, 4)).alias("open_price"),
    F.lit(None).cast(DecimalType(18, 4)).alias("high_price"),
    F.lit(None).cast(DecimalType(18, 4)).alias("low_price"),
    F.col("close_price").cast(DecimalType(18, 4)).alias("close_price"),
    F.lit(0.0).cast(DecimalType(18, 4)).alias("volume"),
    F.lit("BCB_SGS").alias("data_source"),
)

# 3. Leitura e Padronização de Ativos de Mercado do Yahoo Finance
df_yf_raw = spark.read.json(raw_yfinance_path)

df_yf_standardized = df_yf_raw.select(
    F.to_date(F.col("reference_date")).alias("reference_date"),
    F.col("ticker"),
    F.col("open_price").cast(DecimalType(18, 4)).alias("open_price"),
    F.col("high_price").cast(DecimalType(18, 4)).alias("high_price"),
    F.col("low_price").cast(DecimalType(18, 4)).alias("low_price"),
    F.col("close_price").cast(DecimalType(18, 4)).alias("close_price"),
    F.col("volume").cast(DecimalType(18, 4)).alias("volume"),
    F.lit("YAHOO_FINANCE").alias("data_source"),
)

# 4. União e Deduplicação pela Chave de Negócio (reference_date + ticker)
df_union = df_bcb_standardized.unionByName(df_yf_standardized)

# Mantém o registro mais consistente descartando nulos em close_price
w_dedup = Window.partitionBy("reference_date", "ticker").orderBy(F.col("close_price").desc_nulls_last())

df_dedup = (
    df_union.filter(F.col("reference_date").isNotNull() & (F.col("close_price").isNotNull()))
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# 5. Tratamento de Lacunas Temporais via Forward-Fill por Ativo
w_ffill = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(Window.unboundedPreceding, 0)

df_silver_conformed = df_dedup.withColumn(
    "close_price", F.last("close_price", ignorenulls=True).over(w_ffill)
).withColumn("ingestion_timestamp", F.current_timestamp())

# 6. Gravação Idempotente no Delta Lake (macroflow_silver.market_conformed)
target_silver_table = "macroflow_silver.market_conformed"

if not spark.catalog.tableExists(target_silver_table):
    (df_silver_conformed.write.format("delta").partitionBy("ticker").saveAsTable(target_silver_table))
else:
    delta_silver = DeltaTable.forName(spark, target_silver_table)
    (
        delta_silver.alias("target")
        .merge(
            source=df_silver_conformed.alias("source"),
            condition="target.reference_date = source.reference_date AND target.ticker = source.ticker",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
