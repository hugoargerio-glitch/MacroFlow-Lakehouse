import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MacroFlow-Bronze-Ingestion").getOrCreate()

# 1. Caminhos de Leitura dos JSONs Brutos da Ingestão de Borda (Airflow Edge)
raw_bcb_path = "/mnt/raw_data/bcb/*/*/*/*.json"
raw_yfinance_path = "/mnt/raw_data/yfinance/*/*/*/*.json"

# 2. Ingestão da Camada Bronze em Tabelas Delta brutas (Append-only / Raw audit log)
df_bcb_raw = spark.read.json(raw_bcb_path)
(
    df_bcb_raw.withColumn("ingestion_timestamp", F.current_timestamp())
    .write.format("delta")
    .mode("append")
    .saveAsTable("macroflow_bronze.raw_bcb")
)

df_yf_raw = spark.read.json(raw_yfinance_path)
(
    df_yf_raw.withColumn("ingestion_timestamp", F.current_timestamp())
    .write.format("delta")
    .mode("append")
    .saveAsTable("macroflow_bronze.raw_yfinance")
)
