import hashlib
from datetime import timedelta

import mlflow.lightgbm
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MacroFlow-Batch-Inference").getOrCreate()

# 1. Carregamento do Campeão do MLflow Registry
model_name = "MacroFlow_USDBRL_Forecaster"
model_uri = f"models:/{model_name}/Production"
model = mlflow.lightgbm.load_model(model_uri)

client = mlflow.tracking.MlflowClient()
prod_version = client.get_latest_versions(model_name, stages=["Production"])[0].version

# 2. Obtenção do Ponto Temporal Mais Recente (t0) da Feature Store
df_spark = spark.read.table("macroflow_gold.feature_store_fx")
df_latest = df_spark.sort(F.col("reference_date").desc()).limit(1).toPandas()

if df_latest.empty:
    raise ValueError("A Feature Store está vazia. Não é possível gerar inferência.")

ref_date = pd.to_datetime(df_latest["reference_date"].values[0]).date()
current_price = float(df_latest["close_price"].values[0])

feature_cols = [
    "rsi_14",
    "bb_spread_20",
    "ma_divergence_7_30",
    "rolling_vol_30d",
    "rolling_vol_60d",
    "rolling_vol_90d",
    "real_interest_rate",
    "ibov_daily_return",
    "btc_daily_return",
    "lag_close_1",
    "lag_return_1",
    "lag_vol_30d_1",
    "lag_close_3",
    "lag_return_3",
    "lag_vol_30d_3",
    "lag_close_5",
    "lag_return_5",
    "lag_vol_30d_5",
    "lag_close_15",
    "lag_return_15",
    "lag_vol_30d_15",
    "lag_close_30",
    "lag_return_30",
    "lag_vol_30d_30",
]

# Desvio padrão dos resíduos para intervalo de confiança de 95% (~1.96 * sigma)
residual_std = 0.0350  # Estimador empírico calibrado no backtesting

# 3. Projeção Multi-Step Recursiva (t+1 até t+7)
forecast_records = []
current_features = df_latest[feature_cols].copy().astype(float)
simulated_price = current_price

for step in range(1, 8):
    target_date = ref_date + timedelta(days=step)

    # Predição para o horizonte step
    pred = float(model.predict(current_features)[0])

    # Margem de incerteza acumulativa: sigma * sqrt(step)
    uncertainty = 1.96 * residual_std * np.sqrt(step)
    lower_bound = round(pred - uncertainty, 4)
    upper_bound = round(pred + uncertainty, 4)

    # Geração de Chave Primária Hash MD5
    hash_key = f"{ref_date}_{target_date}_1_{prod_version}"
    forecast_id = hashlib.md5(hash_key.encode("utf-8")).hexdigest()

    forecast_records.append(
        {
            "FORECAST_ID": forecast_id,
            "REFERENCE_DATE": str(ref_date),
            "TARGET_HORIZON_DATE": str(target_date),
            "ASSET_ID": 1,  # 1 = USDBRL=X na DIM_ASSET
            "HORIZON_DAYS": step,
            "PREDICTED_VALUE": round(pred, 4),
            "LOWER_BOUND_95": lower_bound,
            "UPPER_BOUND_95": upper_bound,
            "MODEL_VERSION": f"LightGBM_v{prod_version}",
        }
    )

    # Atualização autoregressiva para o próximo passo recursivo
    current_features["lag_close_1"] = pred
    current_features["lag_return_1"] = (pred - simulated_price) / simulated_price
    simulated_price = pred

# 4. Gravação Idempotente no Snowflake (FACT_MACRO_FORECASTS)
df_forecast_spark = spark.createDataFrame(pd.DataFrame(forecast_records))

snowflake_options = {
    "sfURL": "https://<your_account_identifier>.snowflakecomputing.com",
    "sfUser": dbutils.secrets.get(scope="macroflow_scope", key="SNOWFLAKE_USER"),  # noqa: F821
    "sfPassword": dbutils.secrets.get(scope="macroflow_scope", key="SNOWFLAKE_PASSWORD"),  # noqa: F821
    "sfDatabase": "MACROFLOW_DW",
    "sfSchema": "GOLD",
    "sfWarehouse": "COMPUTE_WH",
}

(
    df_forecast_spark.write.format("snowflake")
    .options(**snowflake_options)
    .option("dbtable", "FACT_MACRO_FORECASTS")
    .option("usestagingtable", "ON")
    .mode("append")
    .save()
)
