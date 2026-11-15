-- ====================================================================
-- MacroFlow Lakehouse - Snowflake Infrastructure & RBAC Provisioning
-- Authoritative DDL: Compute, Database, Schema, and Security Roles
-- ====================================================================

-- 1. Provisionamento de Recursos de Infraestrutura e Computacao (SYSADMIN)
USE ROLE SYSADMIN;

-- Criacao do Warehouse com auto-suspend de 60s para otimizacao de creditos
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse analitico padrao para a plataforma MacroFlow Lakehouse';

-- Criacao do Banco de Dados Analitico Central
CREATE DATABASE IF NOT EXISTS MACROFLOW_DW
    COMMENT = 'Data Warehouse analitico central da plataforma MacroFlow';

-- Criacao do Esquema GOLD (Camada de Servico e Dimensional Marts)
CREATE SCHEMA IF NOT EXISTS MACROFLOW_DW.GOLD
    COMMENT = 'Camada de consumo analitico estruturada no modelo dimensional Kimball';

-- 2. Criacao dos Papeis de Acesso de Minimo Privilegio (SECURITYADMIN)
USE ROLE SECURITYADMIN;

-- Papel de servico para os jobs de ingestao e sincronizacao Databricks -> Snowflake
CREATE ROLE IF NOT EXISTS MACROFLOW_ETL_ROLE
    COMMENT = 'Papel de servico para pipelines de carga, transformacao e sync do Lakehouse para o DW';

-- Papel de consulta analitica para consumidores de BI, dashboards e cientistas de dados
CREATE ROLE IF NOT EXISTS MACROFLOW_BI_READER
    COMMENT = 'Papel restrito de somente leitura para analistas e ferramentas de BI';

-- Estabelecimento da Hierarquia RBAC: concede papeis operacionais ao SYSADMIN para governanca
GRANT ROLE MACROFLOW_ETL_ROLE TO ROLE SYSADMIN;
GRANT ROLE MACROFLOW_BI_READER TO ROLE SYSADMIN;

-- 3. Concessao de Privilegios Minimos (Least Privilege Grants)

-- Privilegios para MACROFLOW_ETL_ROLE (Acesso de Escrita/Carga)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MACROFLOW_ETL_ROLE;
GRANT USAGE ON DATABASE MACROFLOW_DW TO ROLE MACROFLOW_ETL_ROLE;
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE VIEW, CREATE FILE FORMAT ON SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_ETL_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_ETL_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_ETL_ROLE;

-- Privilegios para MACROFLOW_BI_READER (Acesso Estritamente Read-Only)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MACROFLOW_BI_READER;
GRANT USAGE ON DATABASE MACROFLOW_DW TO ROLE MACROFLOW_BI_READER;
GRANT USAGE ON SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
