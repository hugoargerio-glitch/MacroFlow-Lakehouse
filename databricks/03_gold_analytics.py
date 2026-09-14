import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("MacroFlow-Gold-Analytics").getOrCreate()

# 1. Leitura dos Dados Conformes da Silver
df_silver = spark.read.table("macroflow_silver.market_conformed")

# 2. Definição das Janelas Analíticas por Ativo
w_lag = Window.partitionBy("ticker").orderBy("reference_date")

# Janelas para Médias Móveis (dias corridos de observação de preço)
w_7d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-6, 0)
w_30d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-29, 0)
w_200d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-199, 0)

# Janelas para Volatilidade Móvel (janelas de N retornos calculados)
w_vol_30d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-29, 0)
w_vol_60d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-59, 0)
w_vol_90d = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-89, 0)

# 3. Transformação Analítica de Séries Temporais
df_analytics = (
    df_silver
    # Defasagem segura do fechamento anterior
    .withColumn("prev_close", F.lag("close_price", 1).over(w_lag))
    # Cálculo do Retorno Simples e Log-Return com proteção prev_close > 0
    .withColumn(
        "daily_return",
        F.when(F.col("prev_close") > 0, (F.col("close_price") - F.col("prev_close")) / F.col("prev_close")),
    )
    .withColumn("log_return", F.when(F.col("prev_close") > 0, F.log(F.col("close_price") / F.col("prev_close"))))
    # Contadores de histórico completo para SMA
    .withColumn("count_7d", F.count("close_price").over(w_7d))
    .withColumn("count_30d", F.count("close_price").over(w_30d))
    .withColumn("count_200d", F.count("close_price").over(w_200d))
    # Contadores de retornos válidos para Volatilidade
    .withColumn("count_vol_30d", F.count("daily_return").over(w_vol_30d))
    .withColumn("count_vol_60d", F.count("daily_return").over(w_vol_60d))
    .withColumn("count_vol_90d", F.count("daily_return").over(w_vol_90d))
    # Médias Móveis Simples (SMA)
    .withColumn("sma_7", F.when(F.col("count_7d") == 7, F.avg("close_price").over(w_7d)))
    .withColumn("sma_30", F.when(F.col("count_30d") == 30, F.avg("close_price").over(w_30d)))
    .withColumn("sma_200", F.when(F.col("count_200d") == 200, F.avg("close_price").over(w_200d)))
    # Volatilidade Móvel Corrigida (>= N observações ativas)
    .withColumn("rolling_vol_30d", F.when(F.col("count_vol_30d") >= 30, F.stddev("daily_return").over(w_vol_30d)))
    .withColumn("rolling_vol_60d", F.when(F.col("count_vol_60d") >= 60, F.stddev("daily_return").over(w_vol_60d)))
    .withColumn("rolling_vol_90d", F.when(F.col("count_vol_90d") >= 90, F.stddev("daily_return").over(w_vol_90d)))
    .drop("prev_close", "count_7d", "count_30d", "count_200d", "count_vol_30d", "count_vol_60d", "count_vol_90d")
)

# 4. Tratamento do Juro Real com Forward-Fill do IPCA
# Cria um grid temporal unificado apenas para as variáveis macroeconômicas
df_macro_dates = df_silver.select("reference_date").distinct()

df_selic = df_silver.filter(F.col("ticker") == "SELIC").select(
    "reference_date", F.col("close_price").alias("selic_rate")
)
df_ipca = df_silver.filter(F.col("ticker") == "IPCA").select("reference_date", F.col("close_price").alias("ipca_rate"))

# Janela temporal ordenada para propagar a última taxa divulgada
w_calendar = Window.orderBy("reference_date").rowsBetween(Window.unboundedPreceding, 0)

df_macro_aligned = (
    df_macro_dates.join(df_selic, on="reference_date", how="left")
    .join(df_ipca, on="reference_date", how="left")
    # Propaga o IPCA mensal para todos os dias subsequentes até a próxima divulgação
    .withColumn("ipca_filled", F.last("ipca_rate", ignorenulls=True).over(w_calendar))
    .withColumn("selic_filled", F.last("selic_rate", ignorenulls=True).over(w_calendar))
    # Juro Real Anualizado Estimado
    .withColumn("real_interest_rate", F.col("selic_filled") - F.col("ipca_filled"))
    .select("reference_date", "real_interest_rate")
)

# 5. Consolidação Final dos Indicadores Analíticos
df_gold_final = df_analytics.join(df_macro_aligned, on="reference_date", how="left").select(
    "reference_date",
    "ticker",
    F.col("close_price").cast(DecimalType(18, 4)),
    F.col("daily_return").cast(DecimalType(12, 6)),
    F.col("log_return").cast(DecimalType(12, 6)),
    F.col("sma_7").cast(DecimalType(18, 4)),
    F.col("sma_30").cast(DecimalType(18, 4)),
    F.col("sma_200").cast(DecimalType(18, 4)),
    F.col("rolling_vol_30d").cast(DecimalType(12, 6)),
    F.col("rolling_vol_60d").cast(DecimalType(12, 6)),
    F.col("rolling_vol_90d").cast(DecimalType(12, 6)),
    F.col("real_interest_rate").cast(DecimalType(10, 4)),
    F.current_timestamp().alias("calculated_at"),
)

# 6. Gravação Idempotente no Delta Lake (macroflow_gold.fact_market_daily)
target_gold_table = "macroflow_gold.fact_market_daily"

if not spark.catalog.tableExists(target_gold_table):
    (df_gold_final.write.format("delta").partitionBy("ticker").saveAsTable(target_gold_table))
else:
    delta_gold = DeltaTable.forName(spark, target_gold_table)
    (
        delta_gold.alias("target")
        .merge(
            source=df_gold_final.alias("source"),
            condition="target.reference_date = source.reference_date AND target.ticker = source.ticker",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
