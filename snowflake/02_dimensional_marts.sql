USE DATABASE MACROFLOW_DW;
USE SCHEMA GOLD;

-- =========================================================
-- 1. ESTRUTURAÇÃO DAS TABELAS DIMENSIONAIS
-- =========================================================

CREATE TABLE IF NOT EXISTS DIM_CALENDAR (
    calendar_date DATE NOT NULL,
    year_number SMALLINT NOT NULL,
    quarter_number SMALLINT NOT NULL,
    month_number SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_trading_day_b3 BOOLEAN NOT NULL,
    is_us_market_open BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_calendar PRIMARY KEY (calendar_date)
);

CREATE TABLE IF NOT EXISTS DIM_ASSET (
    asset_id INTEGER NOT NULL,
    ticker_or_code VARCHAR(30) NOT NULL,
    asset_name VARCHAR(100) NOT NULL,
    asset_class VARCHAR(50) NOT NULL,
    data_source VARCHAR(30) NOT NULL,
    quote_currency VARCHAR(10) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_asset PRIMARY KEY (asset_id),
    CONSTRAINT uq_asset_ticker UNIQUE (ticker_or_code)
);

-- =========================================================
-- 2. ESTRUTURAÇÃO DAS TABELAS DE FATO
-- =========================================================

CREATE TABLE IF NOT EXISTS FACT_MARKET_DAILY (
    reference_date DATE NOT NULL,
    asset_id INTEGER NOT NULL,
    close_price NUMBER(18, 4),
    daily_return NUMBER(12, 6),
    log_return NUMBER(12, 6),
    sma_7 NUMBER(18, 4),
    sma_30 NUMBER(18, 4),
    sma_200 NUMBER(18, 4),
    rolling_vol_30d NUMBER(12, 6),
    rolling_vol_60d NUMBER(12, 6),
    rolling_vol_90d NUMBER(12, 6),
    real_interest_rate NUMBER(10, 4),
    selic_vs_us_spread NUMBER(10, 4),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_market_daily PRIMARY KEY (reference_date, asset_id),
    CONSTRAINT fk_market_calendar FOREIGN KEY (reference_date) REFERENCES DIM_CALENDAR(calendar_date),
    CONSTRAINT fk_market_asset FOREIGN KEY (asset_id) REFERENCES DIM_ASSET(asset_id)
);

CREATE TABLE IF NOT EXISTS FACT_MACRO_FORECASTS (
    forecast_id VARCHAR(64) NOT NULL,
    reference_date DATE NOT NULL,
    target_horizon_date DATE NOT NULL,
    asset_id INTEGER NOT NULL,
    horizon_days SMALLINT NOT NULL,
    predicted_value NUMBER(18, 4) NOT NULL,
    lower_bound_95 NUMBER(18, 4),
    upper_bound_95 NUMBER(18, 4),
    model_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_macro_forecasts PRIMARY KEY (forecast_id),
    CONSTRAINT fk_forecast_ref_cal FOREIGN KEY (reference_date) REFERENCES DIM_CALENDAR(calendar_date),
    CONSTRAINT fk_forecast_tgt_cal FOREIGN KEY (target_horizon_date) REFERENCES DIM_CALENDAR(calendar_date),
    CONSTRAINT fk_forecast_asset FOREIGN KEY (asset_id) REFERENCES DIM_ASSET(asset_id)
);

-- =========================================================
-- 3. POPULAÇÃO DAS DIMENSÕES CONFORMADAS
-- =========================================================

-- Carga Cadastral da DIM_ASSET
MERGE INTO DIM_ASSET AS target
USING (
    SELECT 1 AS asset_id, 'USDBRL=X' AS ticker_or_code, 'Dólar Comercial / Real' AS asset_name, 'FX' AS asset_class, 'YAHOO_FINANCE' AS data_source, 'BRL' AS quote_currency UNION ALL
    SELECT 2, '^BVSP', 'Índice Bovespa', 'Equities', 'YAHOO_FINANCE', 'INDEX_POINTS' UNION ALL
    SELECT 3, 'BTC-BRL', 'Bitcoin / Real', 'Crypto', 'YAHOO_FINANCE', 'BRL' UNION ALL
    SELECT 4, 'USD_PTAX', 'Taxa PTAX BCB', 'FX', 'BCB_SGS', 'BRL' UNION ALL
    SELECT 5, 'SELIC', 'Taxa Selic Over Anualizada', 'Rates', 'BCB_SGS', 'PERCENT_PA' UNION ALL
    SELECT 6, 'IPCA', 'IPCA Acumulado 12 Meses', 'Inflation', 'BCB_SGS', 'PERCENT_PA'
) AS source
ON target.asset_id = source.asset_id
WHEN MATCHED THEN
    UPDATE SET target.ticker_or_code = source.ticker_or_code, target.asset_name = source.asset_name
WHEN NOT MATCHED THEN
    INSERT (asset_id, ticker_or_code, asset_name, asset_class, data_source, quote_currency)
    VALUES (source.asset_id, source.ticker_or_code, source.asset_name, source.asset_class, source.data_source, source.quote_currency);

-- Geração Procedural da DIM_CALENDAR (Janela 2020 a 2030)
INSERT INTO DIM_CALENDAR
WITH date_sequence AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365 * 11))
)
SELECT
    calendar_date,
    YEAR(calendar_date) AS year_number,
    QUARTER(calendar_date) AS quarter_number,
    MONTH(calendar_date) AS month_number,
    MONTHNAME(calendar_date) AS month_name,
    WEEKOFYEAR(calendar_date) AS week_of_year,
    DAYOFWEEK(calendar_date) AS day_of_week,
    DAYNAME(calendar_date) AS day_name,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    -- Regra simplificada de dia de negociação (dias de semana)
    CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN FALSE ELSE TRUE END AS is_trading_day_b3,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN FALSE ELSE TRUE END AS is_us_market_open
FROM date_sequence
WHERE calendar_date <= '2030-12-31'
  AND calendar_date NOT IN (SELECT calendar_date FROM DIM_CALENDAR);
