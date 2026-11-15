-- ====================================================================
-- MacroFlow BI Serving Layer - Query 02: Cross-Asset Rolling Metrics
-- Domain: Multi-Asset Volatility, Trend Following & Regime Classification
-- ====================================================================

-- Contexto de Execucao (pode ser executado com MACROFLOW_BI_READER)
-- USE ROLE MACROFLOW_BI_READER;
-- USE WAREHOUSE COMPUTE_WH;
-- USE DATABASE MACROFLOW_DW;
-- USE SCHEMA GOLD;

WITH asset_daily_metrics AS (
    SELECT
        f.reference_date,
        c.year_number,
        c.month_number,
        c.day_name,
        c.is_trading_day_b3,
        a.asset_id,
        a.ticker_or_code,
        a.asset_name,
        a.asset_class,
        f.close_price,
        f.daily_return,
        f.log_return,
        f.sma_7,
        f.sma_30,
        f.sma_200,
        f.rolling_vol_30d,
        f.rolling_vol_60d,
        f.rolling_vol_90d,
        -- Divergencia de Medias Moveis (Spread percentual SMA 7 vs SMA 30)
        CASE 
            WHEN f.sma_30 > 0 THEN (f.sma_7 - f.sma_30) / f.sma_30 
            ELSE NULL 
        END AS ma_divergence_pct,
        -- Classificacao de Regime de Tendencia
        CASE
            WHEN f.close_price > f.sma_200 AND f.sma_7 > f.sma_30 THEN 'BULLISH_EXPANSION'
            WHEN f.close_price > f.sma_200 AND f.sma_7 <= f.sma_30 THEN 'BULLISH_CORRECTION'
            WHEN f.close_price <= f.sma_200 AND f.sma_7 < f.sma_30 THEN 'BEARISH_CONTRACTION'
            WHEN f.close_price <= f.sma_200 AND f.sma_7 >= f.sma_30 THEN 'BEARISH_REBOUND'
            ELSE 'NEUTRAL'
        END AS technical_regime
    FROM FACT_MARKET_DAILY f
    INNER JOIN DIM_ASSET a 
        ON f.asset_id = a.asset_id
    INNER JOIN DIM_CALENDAR c 
        ON f.reference_date = c.calendar_date
    WHERE a.asset_class IN ('FX', 'Equities', 'Crypto')
      AND c.is_trading_day_b3 = TRUE
)

-- Extracao dos ultimos indicadores com ranking de volatilidade por classe de ativo
SELECT
    reference_date,
    ticker_or_code,
    asset_name,
    asset_class,
    ROUND(close_price, 4) AS close_price,
    ROUND(daily_return * 100, 2) AS daily_return_pct,
    ROUND(rolling_vol_30d * 100, 2) AS annualized_vol_30d_pct,
    ROUND(rolling_vol_90d * 100, 2) AS annualized_vol_90d_pct,
    ROUND(ma_divergence_pct * 100, 2) AS ma_spread_7_30_pct,
    technical_regime
FROM asset_daily_metrics
WHERE reference_date >= DATEADD(MONTH, -3, CURRENT_DATE())
ORDER BY
    reference_date DESC,
    asset_class,
    ticker_or_code;
