-- ====================================================================
-- MacroFlow Lakehouse - Snowflake Dimensional Serving Layer (Kimball Star Schema)
-- Authoritative DDL & Seeding: Conformed Dimensions & Fact Tables
-- ====================================================================

USE ROLE MACROFLOW_ETL_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MACROFLOW_DW;
USE SCHEMA GOLD;

-- ====================================================================
-- 1. ESTRUTURACAO DAS TABELAS DIMENSIONAIS CONFORMADAS
-- ====================================================================

-- Dimensao de Calendario: ancora temporal para todas as analises e forecasts
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

-- Dimensao de Ativos e Series Temporais
CREATE TABLE IF NOT EXISTS DIM_ASSET (
    asset_id INTEGER NOT NULL,
    ticker_or_code VARCHAR(30) NOT NULL,
    asset_name VARCHAR(100) NOT NULL,
    asset_class VARCHAR(50) NOT NULL,
    data_source VARCHAR(30) NOT NULL,
    quote_currency VARCHAR(20) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_asset PRIMARY KEY (asset_id),
    CONSTRAINT uq_asset_ticker UNIQUE (ticker_or_code)
);

-- ====================================================================
-- 2. ESTRUTURACAO DAS TABELAS DE FATO
-- ====================================================================

-- Fato de Mercado Diario: metricas calculadas no Databricks Gold sincronizadas
CREATE TABLE IF NOT EXISTS FACT_MARKET_DAILY (
    reference_date DATE NOT NULL,
    asset_id INTEGER NOT NULL,
    close_price NUMBER(18, 8),
    daily_return NUMBER(12, 6),
    log_return NUMBER(12, 6),
    sma_7 NUMBER(18, 8),
    sma_30 NUMBER(18, 8),
    sma_200 NUMBER(18, 8),
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

-- Fato de Projecoes Macroeconomicas: inferencia batch de modelos de Machine Learning
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

-- ====================================================================
-- 3. CARGA E CONFORMACAO DAS DIMENSOES
-- ====================================================================

-- 3.1 Carga Cadastral Idempotente da DIM_ASSET (incluindo ^IRX e unidades exatas)
MERGE INTO DIM_ASSET AS target
USING (
    SELECT 1 AS asset_id, 'USDBRL=X' AS ticker_or_code, 'Dolar Comercial / Real' AS asset_name, 'FX' AS asset_class, 'YAHOO_FINANCE' AS data_source, 'BRL' AS quote_currency, TRUE AS is_active UNION ALL
    SELECT 2, '^BVSP', 'Indice Bovespa', 'Equities', 'YAHOO_FINANCE', 'INDEX_POINTS', TRUE UNION ALL
    SELECT 3, 'BTC-BRL', 'Bitcoin / Real', 'Crypto', 'YAHOO_FINANCE', 'BRL', TRUE UNION ALL
    SELECT 4, 'USD_PTAX', 'Taxa PTAX BCB', 'FX', 'BCB_SGS', 'BRL', TRUE UNION ALL
    SELECT 5, 'SELIC', 'Taxa Selic Over Diaria', 'Rates', 'BCB_SGS', 'PERCENT_DAY', TRUE UNION ALL
    SELECT 6, 'IPCA', 'IPCA Variacao Mensal', 'Inflation', 'BCB_SGS', 'PERCENT_MONTH', TRUE UNION ALL
    SELECT 7, '^IRX', 'US 13-Week Treasury Bill Yield', 'Rates', 'YAHOO_FINANCE', 'PERCENT_PA', TRUE
) AS source
ON target.asset_id = source.asset_id
WHEN MATCHED THEN
    UPDATE SET 
        target.ticker_or_code = source.ticker_or_code, 
        target.asset_name = source.asset_name,
        target.asset_class = source.asset_class,
        target.data_source = source.data_source,
        target.quote_currency = source.quote_currency,
        target.is_active = source.is_active
WHEN NOT MATCHED THEN
    INSERT (asset_id, ticker_or_code, asset_name, asset_class, data_source, quote_currency, is_active)
    VALUES (source.asset_id, source.ticker_or_code, source.asset_name, source.asset_class, source.data_source, source.quote_currency, source.is_active);

-- 3.2 Geracao Procedural da DIM_CALENDAR (Janela Completa 2020-01-01 a 2030-12-31 = 4.018 dias)
INSERT INTO DIM_CALENDAR (
    calendar_date,
    year_number,
    quarter_number,
    month_number,
    month_name,
    week_of_year,
    day_of_week,
    day_name,
    is_weekend,
    is_trading_day_b3,
    is_us_market_open
)
WITH date_sequence AS (
    SELECT DATEADD(DAY, (ROW_NUMBER() OVER (ORDER BY NULL) - 1), '2020-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
)
SELECT
    calendar_date,
    YEAR(calendar_date) AS year_number,
    QUARTER(calendar_date) AS quarter_number,
    MONTH(calendar_date) AS month_number,
    MONTHNAME(calendar_date) AS month_name,
    WEEKOFYEAR(calendar_date) AS week_of_year,
    DAYOFWEEKISO(calendar_date) AS day_of_week,
    DAYNAME(calendar_date) AS day_name,
    CASE WHEN DAYOFWEEKISO(calendar_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    -- Inicialmente dias uteis (flags serao refinadas com os calendarios de feriados abaixo)
    CASE WHEN DAYOFWEEKISO(calendar_date) IN (6, 7) THEN FALSE ELSE TRUE END AS is_trading_day_b3,
    CASE WHEN DAYOFWEEKISO(calendar_date) IN (6, 7) THEN FALSE ELSE TRUE END AS is_us_market_open
FROM date_sequence
WHERE calendar_date <= '2030-12-31'
  AND calendar_date NOT IN (SELECT calendar_date FROM DIM_CALENDAR);

-- 3.3 Refinamento de Dias de Negociacao B3 (Feriados Nacionais e Feriados Bancarios)
-- Feriados fixos no Brasil
UPDATE DIM_CALENDAR
SET is_trading_day_b3 = FALSE
WHERE (
    (MONTH(calendar_date) = 1 AND DAY(calendar_date) = 1)   -- Confraternizacao Universal
    OR (MONTH(calendar_date) = 4 AND DAY(calendar_date) = 21) -- Tiradentes
    OR (MONTH(calendar_date) = 5 AND DAY(calendar_date) = 1)  -- Dia do Trabalho
    OR (MONTH(calendar_date) = 9 AND DAY(calendar_date) = 7)  -- Independencia do Brasil
    OR (MONTH(calendar_date) = 10 AND DAY(calendar_date) = 12) -- N. Sra. Aparecida
    OR (MONTH(calendar_date) = 11 AND DAY(calendar_date) = 2)  -- Finados
    OR (MONTH(calendar_date) = 11 AND DAY(calendar_date) = 15) -- Proclamacao da Republica
    OR (MONTH(calendar_date) = 11 AND DAY(calendar_date) = 20) -- Consciencia Negra
    OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 24) -- Vespera de Natal (sem negociacao B3)
    OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 25) -- Natal
    OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 31) -- Ultimo dia util do ano (sem expediente B3)
);

-- Feriados moveis no Brasil (Carnaval, Sexta-Feira Santa e Corpus Christi de 2020 a 2030)
UPDATE DIM_CALENDAR
SET is_trading_day_b3 = FALSE
WHERE calendar_date IN (
    -- 2020
    '2020-02-24'::DATE, '2020-02-25'::DATE, '2020-04-10'::DATE, '2020-06-11'::DATE,
    -- 2021
    '2021-02-15'::DATE, '2021-02-16'::DATE, '2021-04-02'::DATE, '2021-06-03'::DATE,
    -- 2022
    '2022-02-28'::DATE, '2022-03-01'::DATE, '2022-04-15'::DATE, '2022-06-16'::DATE,
    -- 2023
    '2023-02-20'::DATE, '2023-02-21'::DATE, '2023-04-07'::DATE, '2023-06-08'::DATE,
    -- 2024
    '2024-02-12'::DATE, '2024-02-13'::DATE, '2024-03-29'::DATE, '2024-05-30'::DATE,
    -- 2025
    '2025-03-03'::DATE, '2025-03-04'::DATE, '2025-04-18'::DATE, '2025-06-19'::DATE,
    -- 2026
    '2026-02-16'::DATE, '2026-02-17'::DATE, '2026-04-03'::DATE, '2026-06-04'::DATE,
    -- 2027
    '2027-02-08'::DATE, '2027-02-09'::DATE, '2027-03-26'::DATE, '2027-05-27'::DATE,
    -- 2028
    '2028-02-28'::DATE, '2028-02-29'::DATE, '2028-04-14'::DATE, '2028-06-15'::DATE,
    -- 2029
    '2029-02-12'::DATE, '2029-02-13'::DATE, '2029-03-30'::DATE, '2029-05-31'::DATE,
    -- 2030
    '2030-03-04'::DATE, '2030-03-05'::DATE, '2030-04-19'::DATE, '2030-06-20'::DATE
);

-- 3.4 Refinamento de Dias de Negociacao nos EUA (NYSE / Nasdaq Market Holidays)
UPDATE DIM_CALENDAR
SET is_us_market_open = FALSE
WHERE (
    -- Ano Novo e observacoes
    (MONTH(calendar_date) = 1 AND DAY(calendar_date) = 1)
    OR (calendar_date IN ('2022-12-30'::DATE, '2023-01-02'::DATE, '2028-01-03'::DATE))
    -- Martin Luther King Jr. Day (3a segunda-feira de janeiro)
    OR (MONTH(calendar_date) = 1 AND DAYOFWEEKISO(calendar_date) = 1 AND DAY(calendar_date) BETWEEN 15 AND 21)
    -- Washington's Birthday / Presidents' Day (3a segunda-feira de fevereiro)
    OR (MONTH(calendar_date) = 2 AND DAYOFWEEKISO(calendar_date) = 1 AND DAY(calendar_date) BETWEEN 15 AND 21)
    -- Memorial Day (Ultima segunda-feira de maio)
    OR (MONTH(calendar_date) = 5 AND DAYOFWEEKISO(calendar_date) = 1 AND DAY(calendar_date) >= 25)
    -- Juneteenth (19 de junho a partir de 2021)
    OR (YEAR(calendar_date) >= 2021 AND (
        (MONTH(calendar_date) = 6 AND DAY(calendar_date) = 19 AND DAYOFWEEKISO(calendar_date) NOT IN (6, 7))
        OR (MONTH(calendar_date) = 6 AND DAY(calendar_date) = 18 AND DAYOFWEEKISO(calendar_date) = 5)
        OR (MONTH(calendar_date) = 6 AND DAY(calendar_date) = 20 AND DAYOFWEEKISO(calendar_date) = 1)
    ))
    -- Independence Day (4 de julho e observacoes)
    OR (
        (MONTH(calendar_date) = 7 AND DAY(calendar_date) = 4 AND DAYOFWEEKISO(calendar_date) NOT IN (6, 7))
        OR (MONTH(calendar_date) = 7 AND DAY(calendar_date) = 3 AND DAYOFWEEKISO(calendar_date) = 5)
        OR (MONTH(calendar_date) = 7 AND DAY(calendar_date) = 5 AND DAYOFWEEKISO(calendar_date) = 1)
    )
    -- Labor Day (1a segunda-feira de setembro)
    OR (MONTH(calendar_date) = 9 AND DAYOFWEEKISO(calendar_date) = 1 AND DAY(calendar_date) BETWEEN 1 AND 7)
    -- Thanksgiving Day (4a quinta-feira de novembro)
    OR (MONTH(calendar_date) = 11 AND DAYOFWEEKISO(calendar_date) = 4 AND DAY(calendar_date) BETWEEN 22 AND 28)
    -- Christmas Day (25 de dezembro e observacoes)
    OR (
        (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 25 AND DAYOFWEEKISO(calendar_date) NOT IN (6, 7))
        OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 24 AND DAYOFWEEKISO(calendar_date) = 5)
        OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 26 AND DAYOFWEEKISO(calendar_date) = 1)
    )
    -- Good Friday nos EUA (2020 a 2030)
    OR calendar_date IN (
        '2020-04-10'::DATE, '2021-04-02'::DATE, '2022-04-15'::DATE, '2023-04-07'::DATE,
        '2024-03-29'::DATE, '2025-04-18'::DATE, '2026-04-03'::DATE, '2027-03-26'::DATE,
        '2028-04-14'::DATE, '2029-03-30'::DATE, '2030-04-19'::DATE
    )
);
