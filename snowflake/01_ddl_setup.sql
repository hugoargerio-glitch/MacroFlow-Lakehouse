-- 1. Garante privilégio administrativo
USE ROLE ACCOUNTADMIN;

-- 2. Cria o Warehouse (Computação com auto-suspend para poupar créditos)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse analitico do MacroFlow';

-- 3. Cria o Banco de Dados e define o contexto
CREATE DATABASE IF NOT EXISTS MACROFLOW_DW;
USE DATABASE MACROFLOW_DW;

-- 4. Cria o Esquema Gold e define o contexto
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- 5. Criação da Tabela de Cotações Diárias e Indicadores
CREATE TABLE IF NOT EXISTS FACT_MARKET_DAILY (
    reference_date DATE NOT NULL,
    ticker_or_code VARCHAR(30) NOT NULL,
    asset_name VARCHAR(100),
    close_price NUMBER(18, 4),
    daily_return NUMBER(10, 6),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (reference_date, ticker_or_code)
);

-- 6. Criação da Tabela de Métricas e Volatilidade
CREATE TABLE IF NOT EXISTS AGG_MACRO_VOLATILITY (
    reference_date DATE NOT NULL,
    asset_name VARCHAR(50) NOT NULL,
    volatility_30d NUMBER(10, 6),
    volatility_60d NUMBER(10, 6),
    volatility_90d NUMBER(10, 6),
    sma_7 NUMBER(18, 4),
    sma_30 NUMBER(18, 4),
    sma_200 NUMBER(18, 4),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (reference_date, asset_name)
);
