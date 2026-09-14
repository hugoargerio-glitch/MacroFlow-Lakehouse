from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("MacroFlow-Feature-Store").getOrCreate()

# 1. Leitura da camada Gold analítica
df_gold = spark.read.table("macroflow_gold.fact_market_daily")

# 2. Isolamento da série alvo principal (USD/BRL)
df_fx = df_gold.filter(F.col("ticker") == "USDBRL=X")

# Janela estritamente temporal para o ativo USD/BRL
w_time = Window.orderBy("reference_date")
w_bb_20 = Window.orderBy("reference_date").rowsBetween(-19, 0)
w_rsi_14 = Window.orderBy("reference_date").rowsBetween(-13, 0)

# 3. Engenharia de Features Técnicas e Autoregressivas
# 3.1 Cálculo do RSI (14 dias)
df_fx_rsi_prep = (
    df_fx
    .withColumn("delta_price", F.col("close_price") - F.lag("close_price", 1).over(w_time))
    .withColumn("gain", F.when(F.col("delta_price") > 0, F.col("delta_price")).otherwise(0.0))
    .withColumn("loss", F.when(F.col("delta_price") < 0, F.abs(F.col("delta_price"))).otherwise(0.0))
    .withColumn("avg_gain_14", F.avg("gain").over(w_rsi_14))
    .withColumn("avg_loss_14", F.avg("loss").over(w_rsi_14))
    .withColumn(
        "rsi_14",
        F.when(F.col("avg_loss_14") == 0, 100.0)
        .otherwise(100.0 - (100.0 / (1.0 + (F.col("avg_gain_14") / F.col("avg_loss_14")))))
    )
)

# 3.2 Bandas de Bollinger e Divergência de Médias
df_fx_technical = (
    df_fx_rsi_prep
    .withColumn("bb_middle_20", F.avg("close_price").over(w_bb_20))
    .withColumn("bb_std_20", F.stddev("close_price").over(w_bb_20))
    .withColumn("bb_upper", F.col("bb_middle_20") + (F.col("bb_std_20") * 2))
    .withColumn("bb_lower", F.col("bb_middle_20") - (F.col("bb_std_20") * 2))
    .withColumn(
        "bb_spread_20",
        F.when(F.col("bb_middle_20") > 0, (F.col("bb_upper") - F.col("bb_lower")) / F.col("bb_middle_20"))
    )
    .withColumn("ma_divergence_7_30", F.col("sma_7") - F.col("sma_30"))
)

# 3.3 Matriz de Lags Autoregressivos (t-1, t-3, t-5, t-15, t-30)
lags = [1, 3, 5, 15, 30]
df_fx_lags = df_fx_technical

for lag_k in lags:
    df_fx_lags = (
        df_fx_lags
        .withColumn(f"lag_close_{lag_k}", F.lag("close_price", lag_k).over(w_time))
        .withColumn(f"lag_return_{lag_k}", F.lag("daily_return", lag_k).over(w_time))
        .withColumn(f"lag_vol_30d_{lag_k}", F.lag("rolling_vol_30d", lag_k).over(w_time))
    )

# 3.4 Target Definition: Fechamento em t+1 (Lead temporal)
df_fx_target = df_fx_lags.withColumn("target_close_d1", F.lead("close_price", 1).over(w_time))

# 4. Cruzamento com Ativos Exógenos (Ibovespa e Bitcoin)
df_exogenous = (
    df_gold.filter(F.col("ticker").isin(["^BVSP", "BTC-BRL"]))
    .groupBy("reference_date")
    .pivot("ticker", ["^BVSP", "BTC-BRL"])
    .agg(F.first("daily_return"))
    .withColumnRenamed("^BVSP", "ibov_daily_return")
    .withColumnRenamed("BTC-BRL", "btc_daily_return")
)

# 5. Consolidação e Limpeza de Borda
df_feature_store = (
    df_fx_target.join(df_exogenous, on="reference_date", how="left")
    # Poda das primeiras 30 linhas necessárias para aquecer os lags (cold start)
    .filter(F.col("lag_close_30").isNotNull())
    .select(
        "reference_date",
        "ticker",
        "close_price",
        "target_close_d1",
        # Momentum e Indicadores Técnicos
        "rsi_14",
        "bb_spread_20",
        "ma_divergence_7_30",
        "rolling_vol_30d",
        "rolling_vol_60d",
        "rolling_vol_90d",
        # Exógenas Macroeconômicas
        "real_interest_rate",
        "ibov_daily_return",
        "btc_daily_return",
        # Lags Autoregressivos
        "lag_close_1", "lag_return_1", "lag_vol_30d_1",
        "lag_close_3", "lag_return_3", "lag_vol_30d_3",
        "lag_close_5", "lag_return_5", "lag_vol_30d_5",
        "lag_close_15", "lag_return_15", "lag_vol_30d_15",
        "lag_close_30", "lag_return_30", "lag_vol_30d_30",
        F.current_timestamp().alias("feature_timestamp")
    )
)

# 6. Gravação na Tabela Delta da Feature Store
feature_table_name = "macroflow_gold.feature_store_fx"

if not spark.catalog.tableExists(feature_table_name):
    df_feature_store.write.format("delta").saveAsTable(feature_table_name)
else:
    delta_fs = DeltaTable.forName(spark, feature_table_name)
    (
        delta_fs.alias("target")
        .merge(
            source=df_feature_store.alias("source"),
            condition="target.reference_date = source.reference_date AND target.ticker = source.ticker"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

