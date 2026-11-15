-- ====================================================================
-- MacroFlow BI Serving Layer - Query 03: Forecast Accuracy & Hedging Bounds
-- Domain: MLOps Telemetry, Residual Drift, and Confidence Coverage
-- ====================================================================

-- Contexto de Execucao (pode ser executado com MACROFLOW_BI_READER)
-- USE ROLE MACROFLOW_BI_READER;
-- USE WAREHOUSE COMPUTE_WH;
-- USE DATABASE MACROFLOW_DW;
-- USE SCHEMA GOLD;

WITH forecast_evaluation AS (
    SELECT
        f.forecast_id,
        f.model_version,
        f.horizon_days,
        f.reference_date AS scoring_date,
        cal_ref.day_name AS scoring_day_name,
        f.target_horizon_date AS target_date,
        cal_tgt.day_name AS target_day_name,
        cal_tgt.is_trading_day_b3 AS target_is_b3_trading,
        a.ticker_or_code,
        a.asset_name,
        -- Valores projetados e bandas de confianca
        f.predicted_value,
        f.lower_bound_95,
        f.upper_bound_95,
        -- Preco realizado observado no fechamento real de mercado da data alvo
        m.close_price AS realized_close_price,
        -- Erros e Residuos
        (m.close_price - f.predicted_value) AS prediction_residual,
        ABS(m.close_price - f.predicted_value) AS absolute_error,
        CASE 
            WHEN m.close_price > 0 THEN ABS(m.close_price - f.predicted_value) / m.close_price 
            ELSE NULL 
        END AS absolute_percentage_error,
        -- Cobertura do Intervalo de Confianca de 95%
        CASE 
            WHEN m.close_price BETWEEN f.lower_bound_95 AND f.upper_bound_95 THEN 1 
            ELSE 0 
        END AS is_within_95_bounds
    FROM FACT_MACRO_FORECASTS f
    INNER JOIN DIM_ASSET a 
        ON f.asset_id = a.asset_id
    INNER JOIN DIM_CALENDAR cal_ref 
        ON f.reference_date = cal_ref.calendar_date
    INNER JOIN DIM_CALENDAR cal_tgt 
        ON f.target_horizon_date = cal_tgt.calendar_date
    -- Inner join com os precos realizados efetivamente consolidados no fato diario
    INNER JOIN FACT_MARKET_DAILY m 
        ON f.target_horizon_date = m.reference_date 
       AND f.asset_id = m.asset_id
)

-- Sumario Telemetrico Agregado por Versao do Modelo e Horizonte de Projecao
SELECT
    model_version,
    horizon_days,
    COUNT(forecast_id) AS evaluated_forecasts_count,
    ROUND(AVG(absolute_error), 4) AS mae,
    ROUND(SQRT(AVG(POWER(prediction_residual, 2))), 4) AS rmse,
    ROUND(AVG(absolute_percentage_error) * 100, 2) AS mape_pct,
    ROUND(SUM(is_within_95_bounds) / COUNT(forecast_id) * 100, 1) AS coverage_95_pct,
    ROUND(MIN(prediction_residual), 4) AS max_underestimation_residual,
    ROUND(MAX(prediction_residual), 4) AS max_overestimation_residual
FROM forecast_evaluation
GROUP BY
    model_version,
    horizon_days
ORDER BY
    model_version,
    horizon_days ASC;
