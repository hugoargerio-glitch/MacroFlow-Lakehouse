-- ====================================================================
-- MacroFlow BI Serving Layer - Query 01: Macro Risk & Sovereign Spread
-- Domain: Fixed Income, Monetary Policy, and FX Dynamics
-- ====================================================================

-- Contexto de Execucao (pode ser executado com MACROFLOW_BI_READER)
-- USE ROLE MACROFLOW_BI_READER;
-- USE WAREHOUSE COMPUTE_WH;
-- USE DATABASE MACROFLOW_DW;
-- USE SCHEMA GOLD;

WITH daily_market AS (
    SELECT
        f.reference_date,
        c.year_number,
        c.quarter_number,
        c.month_number,
        c.month_name,
        c.is_trading_day_b3,
        a.ticker_or_code,
        a.asset_name,
        f.close_price,
        f.daily_return,
        f.rolling_vol_30d,
        f.rolling_vol_60d,
        f.rolling_vol_90d,
        f.real_interest_rate,
        f.selic_vs_us_spread
    FROM FACT_MARKET_DAILY f
    INNER JOIN DIM_ASSET a 
        ON f.asset_id = a.asset_id
    INNER JOIN DIM_CALENDAR c 
        ON f.reference_date = c.calendar_date
    WHERE a.ticker_or_code IN ('USDBRL=X', 'SELIC', 'IPCA', '^IRX')
),

-- Isolamento do Ativo Alvo Cambial (USD/BRL)
fx_daily AS (
    SELECT
        reference_date,
        year_number,
        quarter_number,
        month_number,
        month_name,
        close_price AS usd_brl_close,
        daily_return AS usd_brl_daily_return,
        rolling_vol_30d AS usd_brl_vol_30d,
        real_interest_rate,
        selic_vs_us_spread
    FROM daily_market
    WHERE ticker_or_code = 'USDBRL=X'
)

-- Agregacao Mensal para Relatorios Executivos de Risco e Tesouraria
SELECT
    year_number,
    month_number,
    month_name,
    COUNT(DISTINCT reference_date) AS trading_days_count,
    ROUND(AVG(usd_brl_close), 4) AS avg_usd_brl,
    ROUND(MIN(usd_brl_close), 4) AS min_usd_brl,
    ROUND(MAX(usd_brl_close), 4) AS max_usd_brl,
    ROUND(AVG(usd_brl_vol_30d) * 100, 2) AS avg_annualized_vol_30d_pct,
    ROUND(AVG(real_interest_rate), 4) AS avg_real_interest_rate,
    ROUND(AVG(selic_vs_us_spread), 4) AS avg_selic_vs_us_spread,
    -- Variacao acumulada do cambio no periodo
    ROUND(
        (MAX(CASE WHEN rn_desc = 1 THEN usd_brl_close END) - 
         MAX(CASE WHEN rn_asc = 1 THEN usd_brl_close END)) /
         NULLIF(MAX(CASE WHEN rn_asc = 1 THEN usd_brl_close END), 0) * 100, 
        2
    ) AS monthly_fx_depreciation_pct
FROM (
    SELECT
        fx.*,
        ROW_NUMBER() OVER (PARTITION BY year_number, month_number ORDER BY reference_date ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY year_number, month_number ORDER BY reference_date DESC) AS rn_desc
    FROM fx_daily fx
)
GROUP BY
    year_number,
    month_number,
    month_name
ORDER BY
    year_number DESC,
    month_number DESC;
