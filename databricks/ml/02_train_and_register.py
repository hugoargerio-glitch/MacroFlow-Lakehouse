import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
from pyspark.sql import SparkSession
from mlflow.tracking import MlflowClient

spark = SparkSession.builder.appName("MacroFlow-Model-Training").getOrCreate()

# 1. Carregamento da Feature Store
df_spark = spark.read.table("macroflow_gold.feature_store_fx")
df_data = df_spark.filter(df_spark.target_close_d1.isNotNull()).toPandas()

# Ordenação temporal mandatória
df_data["reference_date"] = pd.to_datetime(df_data["reference_date"])
df_data = df_data.sort_values("reference_date").reset_index(drop=True)

# 2. Definição da Matriz de Features e Target
feature_cols = [
    "rsi_14", "bb_spread_20", "ma_divergence_7_30",
    "rolling_vol_30d", "rolling_vol_60d", "rolling_vol_90d",
    "real_interest_rate", "ibov_daily_return", "btc_daily_return",
    "lag_close_1", "lag_return_1", "lag_vol_30d_1",
    "lag_close_3", "lag_return_3", "lag_vol_30d_3",
    "lag_close_5", "lag_return_5", "lag_vol_30d_5",
    "lag_close_15", "lag_return_15", "lag_vol_30d_15",
    "lag_close_30", "lag_return_30", "lag_vol_30d_30"
]
target_col = "target_close_d1"

X = df_data[feature_cols].astype(float)
y = df_data[target_col].astype(float)

# 3. Split Temporal Out-of-Time (Últimos 60 dias de pregão para teste)
test_size = 60
X_train, X_test = X.iloc[:-test_size], X.iloc[-test_size:]
y_train, y_test = y.iloc[:-test_size], y.iloc[-test_size:]

# 4. Pipeline de Treinamento com MLflow Tracking
EXPERIMENT_NAME = "/Shared/macroflow_usdbrl_forecaster"
mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name="lgbm_recursive_fx_run") as run:
    params = {
        "objective": "regression",
        "metric": "rmse",
        "n_estimators": 400,
        "learning_rate": 0.03,
        "max_depth": 5,
        "num_leaves": 31,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42
    }
    mlflow.log_params(params)

    # Treinamento
    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
    )

    # Avaliação de Desempenho
    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred)
    
    # Directional Accuracy Ratio (Acerto de direção de subida/descida)
    actual_diff = np.sign(y_test.values - X_test["lag_close_1"].values)
    pred_diff = np.sign(y_pred - X_test["lag_close_1"].values)
    directional_accuracy = (actual_diff == pred_diff).mean()

    # Log de Métricas e Telemetria
    mlflow.log_metric("rmse", float(rmse))
    mlflow.log_metric("mae", float(mae))
    mlflow.log_metric("mape", float(mape))
    mlflow.log_metric("directional_accuracy", float(directional_accuracy))

    # Log do Modelo
    model_info = mlflow.lightgbm.log_model(
        lgb_model=model,
        artifact_path="model",
        registered_model_name="MacroFlow_USDBRL_Forecaster"
    )

    # 5. Critério de Benchmark para Promoção para Produção
    client = MlflowClient()
    latest_version = client.get_latest_versions("MacroFlow_USDBRL_Forecaster", stages=["None"])[0].version
    
    # Limiar: MAPE abaixo de 2% para câmbio curto prazo
    if mape < 0.02:
        client.set_registered_model_alias("MacroFlow_USDBRL_Forecaster", "champion", latest_version)
        client.transition_model_version_stage(
            name="MacroFlow_USDBRL_Forecaster",
            version=latest_version,
            stage="Production",
            archive_existing_versions=True
        )
