# 🌐 MacroFlow: Enterprise Macroeconomic & FX Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark%20%7C%20Delta%20Lake-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-Automated%20CI-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![MLflow](https://img.shields.io/badge/MLflow-Experiment%20Tracking%20%26%20Registry-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)](https://mlflow.org/)
[![LightGBM](https://img.shields.io/badge/LightGBM-Time--Series%20Forecasting-FFA100?style=for-the-badge&logo=scikitlearn&logoColor=white)](https://lightgbm.readthedocs.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dimensional%20Analytics-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

An automated, enterprise-grade data platform engineered to ingest, validate, model, forecast, and serve macroeconomic indicators, sovereign debt spreads, and foreign exchange rates.

The platform integrates public APIs from the **Central Bank of Brazil (BCB/SGS)** and financial APIs (**Yahoo Finance**), enforcing runtime data contracts at ingestion with **Pydantic v2**, orchestrating multi-stage execution with **Apache Airflow**, processing data through an ACID **Medallion Architecture (Bronze, Silver, Gold)** on **Databricks (PySpark + Delta Lake)**, operationalizing **Direct Multi-Horizon LightGBM forecasting with Conformal 95% Prediction Intervals (MLflow)**, and synchronizing Kimball-modeled dimensional data marts to **Snowflake** for executive **Business Intelligence (BI)** consumption.

---

## 🚦 Project Implementation Status

| Milestone / Layer | Description | Engine / Tech | Status |
| :--- | :--- | :--- | :--- |
| **01. Ingestion & Edge Contracts** | Multi-source parallel extraction (BCB SGS 11/433/1 + YFinance `USDBRL=X`, `^BVSP`, `BTC-BRL`, `^IRX`), dynamic sliding windows (60d IPCA lookback), Pydantic v2 runtime validation with companion PySpark `StructType` contracts, NDJSON serialization (`allow_nan=False`), and automated sync to Databricks UC Volumes / DBFS. | Apache Airflow, Python 3.11, Pydantic v2, Requests, Requests Retry Adapter | `DONE` ✅ |
| **02. Warehouse Provisioning & RBAC** | Authoritative DDL setup for analytical marts, `COMPUTE_WH` with auto-suspension (60s), least-privilege RBAC roles (`SYSADMIN`, `SECURITYADMIN`, `MACROFLOW_ETL_ROLE`, `MACROFLOW_BI_READER`), 4,018-day `DIM_CALENDAR` (2020–2030) resolving B3 and US market holidays, and `DIM_ASSET` (SCD Type 1) with exact unit metadata. | Snowflake SQL, RBAC | `DONE` ✅ |
| **03. Silver Layer (Lakehouse)** | Incremental Bronze Delta table ingestion, schema normalization, domain sanity validation, deterministic deduplication by ingestion timestamp (`bronze_ingestion_timestamp DESC, source_file DESC`), `Decimal(18, 8)` rate precision, and Delta Lake `MERGE INTO` on `(reference_date, ticker)`. | Databricks, PySpark, Delta Lake | `DONE` ✅ |
| **04. Gold Layer (Feature Store & Marts)** | Pure PySpark window metrics, asset-appropriate volatility annualization ($\sqrt{252}$ for market assets vs $\sqrt{365}$ for 24/7 crypto), macro rate separation, 40-day IPCA publication lag guard, annualized Fisher real interest rate formula, and SELIC vs US Treasury yield spread (`selic_vs_us_spread`). | Databricks, PySpark SQL, Delta Lake | `DONE` ✅ |
| **05. Serving Layer (Kimball Marts)** | Dedicated `04_snowflake_sync.py` pipeline, business ticker to `asset_id` surrogate key resolution, strict typing, and staged transactional `MERGE INTO MACROFLOW_DW.GOLD.FACT_MARKET_DAILY` authenticated via Databricks Secrets / environment credentials. | Snowflake Spark Connector, PySpark, Snowflake SQL | `DONE` ✅ |
| **06. MLOps Batch Inference Engine** | Feature store generation (`macroflow_gold.feature_store_fx`), stationary multi-horizon log-return targets ($h \in \{1..7\}$), expanding-window walk-forward CV (4 folds), Direct Multi-Horizon LightGBM point + quantile models, finite-sample Conformalized Quantile Regression (95% bounds), MLflow `@champion` / `@challenger` aliases, and Airflow-triggered batch scoring advancing B3 trading days with staged Snowflake `MERGE INTO FACT_MACRO_FORECASTS`. | LightGBM, MLflow, Scikit-Learn, PySpark, Apache Airflow | `DONE` ✅ |
| **07. Executive BI & Observability** | Versioned semantic SQL serving queries in `bi/queries/` (macro risk spreads, cross-asset rolling correlations, forecast-vs-actual evaluation), Ruff linting/formatting, Mypy static type checking, and Pytest test suite with an automated 80% coverage gate in GitHub Actions CI. Visual dashboard templates (`bi/dashboards/` for Power BI / Streamlit) are scheduled for subsequent UI deployment. | Snowflake SQL, GitHub Actions, Pytest, Ruff, Mypy | `PARTIALLY IMPLEMENTED` 🟡<br>*(Queries & CI `DONE` ✅, Dashboards `PLANNED` ⚪)* |

---

## 🎯 Key Architectural Highlights

* **Shift-Left Data Contracts:** Runtime data contract validation at edge ingestion using **Pydantic v2** (`BCBRecord`, `MarketRecord`) enforcing strict domain boundaries, forbidding `NaN`/`Inf`, and validating OHLC ordering before payloads ever reach cloud compute clusters. Companion PySpark `StructType` contracts (`BCB_SPARK_SCHEMA`, `MARKET_SPARK_SCHEMA`) ensure zero `_corrupt_record` parsing failures.
* **Connected End-to-End Orchestration:** **Apache Airflow** DAGs orchestrate parallel extraction tasks, stream validated NDJSON payloads to cloud object storage (Databricks UC Volumes / DBFS), trigger the sequential Medallion pipeline (`01_bronze` ➔ `02_silver` ➔ `03_gold`), trigger Snowflake Gold synchronization (`04_snowflake_sync`), and dispatch batch MLOps forecasting (`macroflow_ml_inference_batch`).
* **Mathematically Sound Financial & Macro Semantics:**
  * **Fisher Equation:** Real interest rates are computed via the rigorous Fisher formula $(1 + r_{\text{real}}) = \frac{1 + \text{SELIC}_{\text{ann}}}{1 + \text{IPCA}_{12m}}$, compounding daily SELIC over 252 business days and rolling monthly IPCA over 12 months.
  * **Publication Lag Guard:** Monthly IPCA is shifted forward by an explicit 40-day publication lag, eliminating lookahead bias from historical backtests and feature stores.
  * **Asset-Specific Annualization:** Volatilities are annualized based on asset operating cadences ($\sqrt{252}$ for equities/FX, $\sqrt{365}$ for 24/7 crypto via epoch-second `rangeBetween` windows), while macro policy rates preserve null return/volatility metrics to prevent invalid rate-on-rate variance math.
  * **Sovereign Yield Spread:** Computes $\text{SELIC}_{\text{ann}} - \text{IRX}_{\text{ann}}$ directly against the US 13-Week Treasury Bill yield (`^IRX`).
* **Direct Multi-Horizon MLOps Architecture:** Replaces error-accumulating recursive prediction loops with independent **Direct Multi-Horizon LightGBM models** ($h=1..7$), predicting stationary log-returns $\ln(P_{t+h}/P_t)$ with spot-anchored price reconstruction $\hat{P}_{t+h} = P_t \exp(\hat{r}_{t+h})$.
* **Calibrated Conformal Bounds & MLflow Model Governance:** Replaces arbitrary static error bands with finite-sample **Conformalized Quantile Regression (CQR)**, delivering mathematically guaranteed 95% prediction intervals. Models are registered via MLflow `@challenger` / `@champion` aliases, enforcing benchmark promotion gates against naive random-walk baselines with zero test-set leakage.
* **Enterprise Snowflake Serving & RBAC:** Authoritative Kimball Star Schema in Snowflake featuring a 4,018-day calendar (2020–2030) incorporating Brazilian B3 and US market holidays, least-privilege role segregation (`MACROFLOW_ETL_ROLE`, `MACROFLOW_BI_READER`), and staged transactional `MERGE` statements guaranteeing idempotency on pipeline retries.

---

## 🏗️ System Architecture

```text
                               [DATA SOURCES]
              ┌───────────────────────┴───────────────────────┐
              │                                               │
       [BCB SGS API]                                   [Yahoo Finance API]
  (IPCA, Selic, USD PTAX)                         (USDBRL=X, ^BVSP, BTC-BRL, ^IRX)
              │                                               │
              └───────────────────────┬───────────────────────┘
                                      │
                                      ▼
                     [ AIRFLOW EDGE INGESTION LAYER ]
                     • Parallel Extractors (urllib3 Retries & Error Isolation)
                     • Shift-Left Contracts (Pydantic v2 allow_inf_nan=False)
                     • NDJSON Streaming Sink (allow_nan=False)
                     • Cloud Storage Uploader (UC Volumes / DBFS)
                                      │
                                      ▼
         ┌────────────────────────────┬──────────────────────────────┐
         │                    DATABRICKS LAKEHOUSE                   │
         │                                                           │
         │ 🥉 BRONZE: Raw Delta Tables (macroflow_bronze.*)          │
         │            Incremental partition reads from raw NDJSON    │
         │                             │                             │
         │ 🥈 SILVER: Cleaned & Conformed (macroflow_silver.*)       │
         │            Timestamp dedup, Decimal(18,8), Delta MERGE    │
         │                             │                             │
         │ 🥇 GOLD: Aggregated Analytics (macroflow_gold.*)          │
         │          Fisher Real Rate, Volatility (252d/365d), Spread │
         └──────────────┬────────────────────────────┬───────────────┘
                        │                            │
              [ FEATURE EXTRACTION ]          [ SNOWFLAKE SYNC ]
              (Lags, RSI, CQR Bounds)         (04_snowflake_sync.py)
                        │                            │
                        ▼                            ▼
         ┌───────────────────────────────┐ ┌───────────────────────────────┐
         │         MLOPS ENGINE          │ │      SNOWFLAKE WAREHOUSE      │
         │ • Feature Store (macroflow_gold│ │ • RBAC: MACROFLOW_ETL_ROLE    │
         │ • 4-Fold Walk-Forward CV      │ │         MACROFLOW_BI_READER   │
         │ • Direct Multi-Horizon LGBM   │ │                               │
         │ • Conformal 95% Bounds        │ │ 🏛️ KIMBALL DIMENSIONAL MARTS  │
         │ • MLflow @champion Aliases    │ │ • DIM_CALENDAR (4018d + Hols) │
         │ • D+1 Batch Inference DAG     │ │ • DIM_ASSET (SCD Type 1)      │
         │ • Staged Forecast Upserts     │ │ • FACT_MARKET_DAILY (14 cols) │
         └───────────────┬───────────────┘ │ • FACT_MACRO_FORECASTS        │
                         │ (Predictions)   └───────────────┬───────────────┘
                         └─────────────────────────────────┘
                                         │
                                         ▼
                         ┌─────────────────────────────────┐
                         │   BI & ANALYTICAL CONSUMPTION   │
                         │ • Versioned SQL (bi/queries/)   │
                         │ • Macro Risk & Sovereign Spread │
                         │ • Cross-Asset Rolling Dynamics  │
                         │ • Actual vs Forecast Residuals  │
                         │ • Dashboards (bi/dashboards ⚪) │
                         └─────────────────────────────────┘
```

---

## 📊 Medallion Architecture Breakdown

### 🥉 Bronze Layer (Raw Ingestion)

* **Sources:**
  * Central Bank of Brazil (SGS API): Series `433` (Monthly IPCA, 60-day sliding lookback), `11` (Daily Selic, 7-day lookback), `1` (USD PTAX, 7-day lookback).
  * Yahoo Finance API: Daily OHLCV for `USDBRL=X`, `^BVSP`, `BTC-BRL`, and `^IRX` (US 13-Week Treasury Bill yield).
* **Format:** Strict Newline-Delimited JSON (NDJSON, one JSON object per line, `allow_nan=False`), avoiding the corrupt multi-line array parsing crashes of default Spark readers.
* **Storage:** Synced to Databricks Unity Catalog Volumes (`/Volumes/macroflow/bronze/raw_data`) or DBFS paths partitioned by `source/year=YYYY/month=MM/day=DD`.
* **Governance:** Ingested into append-only Bronze Delta tables (`macroflow_bronze.raw_bcb`, `macroflow_bronze.raw_yfinance`) via partition-scoped batch execution or Databricks Auto Loader (`cloudFiles`), attaching ingestion timestamps, partition dates, and source file metadata while preserving raw payloads.

### 🥈 Silver Layer (Cleaned & Conformed)

* **Source of Truth:** Reads incrementally from Bronze Delta tables (`macroflow_bronze.*`) using watermark filtering, ensuring full lineage and eliminating raw JSON rescans.
* **Schema Enforcement & Precision:**
  * Casts reference dates to `DateType` and converts string tickers to trimmed uppercase representations.
  * Casts prices and rates to `DecimalType(18, 8)`, preserving the full precision of daily SELIC rates (e.g., `0.045073%`), avoiding truncation.
  * Sanitizes numeric fields with defensive finite-float checks, converting `NaN`, `+Inf`, and `-Inf` to `NULL`.
* **Domain Validation:** Filters out non-positive `USD_PTAX`, negative nominal `SELIC`, and IPCA monthly contractions exceeding $-100\%$.
* **Deterministic Deduplication:** Partitions by `(reference_date, ticker)` and orders strictly by `bronze_ingestion_timestamp DESC, source_file DESC`, guaranteeing that late-arriving revisions overwrite older data rather than keeping arbitrary maximum prices.
* **Idempotency:** Liquid-clustered Delta `MERGE INTO macroflow_silver.market_conformed` matched on `target.reference_date = source.reference_date AND target.ticker = source.ticker`.

### 🥇 Gold Layer (Aggregations & Analytics)

* **Separation of Cadences:** Segregates assets into trading-day equities/FX, 24/7 cryptocurrency, and macro sovereign policy rates:
  * **Trading-Day Assets (`USDBRL=X`, `^BVSP`, `USD_PTAX`):** Computes log returns $\ln(P_t/P_{t-1})$ (with positive price guards), moving averages (SMA 7, 30, 200), and rolling standard deviations (30, 60, 90 days) annualized by $\sqrt{252}$:
    $$\sigma_{30\text{d, ann}} = \text{std}(r_{t-29 \dots t}) \times \sqrt{252}$$
  * **24/7 Crypto Assets (`BTC-BRL`):** Uses epoch-second calendar windows (`rangeBetween(-29 * 86400, 0)`) to accommodate continuous calendar trading, annualizing volatility by $\sqrt{365}$:
    $$\sigma_{30\text{d, ann}} = \text{std}(r_{t-29 \dots t}) \times \sqrt{365}$$
  * **Macro Policy Rates (`SELIC`, `IPCA`, `^IRX`):** Preserves null returns, SMAs, and volatilities to prevent invalid rate-on-rate variance calculations.
* **Annualized Compounding & Fisher Real Interest Rate:**
  * Daily SELIC ($r_d$) is compounded to an effective annual rate (% p.a.):
    $$\text{SELIC}_{\text{ann}} = \left[\left(1 + \frac{r_d}{100}\right)^{252} - 1\right] \times 100$$
  * Monthly IPCA ($r_m$) is compounded over a rolling 12-month window:
    $$\text{IPCA}_{12m} = \left[\prod_{m=1}^{12} \left(1 + \frac{r_m}{100}\right) - 1\right] \times 100$$
    and shifted forward by an explicit 40-day publication lag (`DEFAULT_IPCA_PUBLICATION_LAG_DAYS = 40`) to prevent lookahead bias.
  * Real interest rate is calculated via the Fisher equation:
    $$r_{\text{real}} = \left[\frac{1 + \frac{\text{SELIC}_{\text{ann}}}{100}}{1 + \frac{\text{IPCA}_{12m}}{100}} - 1\right] \times 100$$
* **Sovereign Yield Spread:** Computes the interest rate differential between Brazil's annualized SELIC and the US 13-Week Treasury Bill yield:
  $$\text{selic\_vs\_us\_spread} = \text{SELIC}_{\text{ann}} - \text{IRX}_{\text{ann}}$$
* **Serving Sync:** `databricks/04_snowflake_sync.py` maps business tickers to surrogate dimension keys (`DIM_ASSET.asset_id`) and executes an idempotent staged `MERGE INTO MACROFLOW_DW.GOLD.FACT_MARKET_DAILY`.

---

## ❄️ Snowflake Data Warehouse Setup & RBAC

Analytical models and security roles are provisioned in Snowflake with automatic credit suspension (`AUTO_SUSPEND = 60`) and least-privilege role-based access control.

### 1. RBAC Hierarchy & Privileges (`01_ddl_setup.sql`)

```sql
-- Role-Based Access Control Provisioning
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS MACROFLOW_ETL_ROLE
    COMMENT = 'Service role for Databricks Lakehouse pipelines and staged syncs';
CREATE ROLE IF NOT EXISTS MACROFLOW_BI_READER
    COMMENT = 'Read-only role for BI dashboards, semantic queries, and analysts';

GRANT ROLE MACROFLOW_ETL_ROLE TO ROLE SYSADMIN;
GRANT ROLE MACROFLOW_BI_READER TO ROLE SYSADMIN;

-- Operational Grants: MACROFLOW_ETL_ROLE (Read/Write)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MACROFLOW_ETL_ROLE;
GRANT USAGE ON DATABASE MACROFLOW_DW TO ROLE MACROFLOW_ETL_ROLE;
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE VIEW ON SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_ETL_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_ETL_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_ETL_ROLE;

-- Analytical Grants: MACROFLOW_BI_READER (Strictly Read-Only)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MACROFLOW_BI_READER;
GRANT USAGE ON DATABASE MACROFLOW_DW TO ROLE MACROFLOW_BI_READER;
GRANT USAGE ON SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MACROFLOW_DW.GOLD TO ROLE MACROFLOW_BI_READER;
```

### 2. Analytical Fact DDL (`02_dimensional_marts.sql`)

```sql
USE ROLE MACROFLOW_ETL_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MACROFLOW_DW;
USE SCHEMA GOLD;

-- Kimball Star Schema: Market Fact Table
CREATE TABLE IF NOT EXISTS MACROFLOW_DW.GOLD.FACT_MARKET_DAILY (
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
```

---

## 🤖 Production MLOps & Time-Series Forecasting Engine

Rather than treating Machine Learning as an isolated notebook experiment, MacroFlow operationalizes forecasting through a robust **MLOps lifecycle**. Curated Gold tables are transformed into a point-in-time consistent **Feature Store**, training is executed across temporal cross-validation folds, experiment lineage is tracked in **MLflow**, and scheduled batch inference is orchestrated via **Apache Airflow**.

```text
[ Databricks Gold Layer (fact_market_daily) ]
           │
           ▼
[ 1. Feature Store Engine ]   ──▶ Multi-Horizon Log-Return Targets (h=1..7), Lags, Wilder RSI, BB Spread
           │
           ▼
[ 2. Temporal CV & CQR   ]   ──▶ 4-Fold Walk-Forward CV, Direct LightGBM, Conformal 95% Bounds
           │
           ▼
[ 3. MLflow Model Registry]   ──▶ @challenger Evaluation vs Naive Random Walk & Promotion to @champion
           │
           ▼
[ 4. Airflow Inference DAG]   ──▶ Daily D+1 Batch Scoring Job (macroflow_ml_inference_batch)
           │
           ▼
[ 5. Snowflake Staged Sync]   ──▶ Staged MERGE into FACT_MACRO_FORECASTS (B3 Trading Day Horizons)
```

### 1. Feature Engineering & Feature Store (`databricks/ml/01_feature_store.py`)

The feature store transforms curated financial series into a point-in-time consistent matrix:
* **Multi-Horizon Stationary Targets:** Direct targets predicting log-returns over horizons $h \in \{1 \dots 7\}$:
  $$y_{t, h} = \ln\left(\frac{P_{t+h}}{P_t}\right)$$
  Predicting stationary returns eliminates the bounded saturation and mean-reversion failure of models trained on non-stationary price levels.
* **Autoregressive Lags:** Captures multi-period momentum via return and price lags at intervals $t-1, t-3, t-5, t-15, t-30$, with cold-start rows safely pruned.
* **Technical Oscillators:**
  * **Wilder's RSI (14-day):** Computes exponentially smoothed upward and downward price changes, including a defensive flat-price guard ($50.0$ when average gain and average loss are both zero).
  * **Bollinger Band Spread (20-day):** Measures $( \text{Upper Band} - \text{Lower Band} ) / \text{SMA}_{20}$.
  * **Normalized Moving Average Divergence:** Computes $(\text{SMA}_7 - \text{SMA}_{30}) / \text{SMA}_{30}$.
* **Exogenous Alignment:** As-of temporal alignment joins daily returns for Ibovespa (`^BVSP`), Bitcoin (`BTC-BRL`), US Treasury yield (`^IRX`), real interest rate, and sovereign spread without lookahead leakage.

### 2. Model Training & MLflow Governance (`databricks/ml/02_train_and_register.py`)

* **Direct Multi-Horizon Architecture:** Trains separate LightGBM models $f_h(X_t)$ for each horizon $h \in \{1 \dots 7\}$, completely avoiding recursive multi-step feature feedback, frozen lagged states, and compounding prediction errors.
* **Leakage-Free Cross-Validation:** Employs an expanding-window **Walk-Forward Cross-Validation** strategy (4 folds) and a dedicated holdout test split ($\text{Train} < \text{Val} < \text{Test}$). Early stopping is evaluated strictly on the validation set, guaranteeing zero test-set data leakage.
* **Conformalized Quantile Regression (CQR):** Trains point estimators (`objective="regression"`) alongside pinball quantile estimators ($\alpha = 0.025, 0.975$). Computes finite-sample non-conformity adjustments on holdout residuals to guarantee empirical 95% coverage intervals $[L_{t+h}, U_{t+h}]$.
* **Registry & Promotion Gates:** Models are logged with signatures and packaged as `DirectMultiHorizonFXModel` (`mlflow.pyfunc.PythonModel`). The candidate is registered with alias `@challenger` and promoted to `@champion` only if outperforming both the existing `@champion` and a naive random-walk baseline ($P_{t+h} = P_t$) on holdout MAPE and Directional Accuracy. Zero deprecated `stages` are used.

### 3. Automated Batch Scoring Pipeline (`databricks/ml/03_batch_predict.py`)

1. **Airflow Trigger:** The daily ingestion DAG completes Gold transformations and emits a trigger to the `macroflow_ml_inference_batch` DAG.
2. **Model Loading:** The inference engine resolves the current champion model dynamically via alias (`models:/MacroFlow_USDBRL_Forecaster@champion`).
3. **Trading Calendar Stepping:** Forecast target dates are projected along actual **B3 trading days** (using Brazilian holiday logic matching `DIM_CALENDAR`), ensuring that weekends and national holidays are correctly skipped.
4. **Spot Price Reconstruction:** Forecast log-returns $\hat{y}_{t, h}$ are transformed into future price levels:
   $$\hat{P}_{t+h} = P_t \times \exp(\hat{y}_{t, h})$$
   with monotonic 95% conformal confidence bounds $[L_{t+h}, U_{t+h}]$.
5. **Idempotent Serving Load:** Scores are written to staging table `STG_FACT_MACRO_FORECASTS` and upserted via a transactional `MERGE INTO FACT_MACRO_FORECASTS` keyed on surrogate `forecast_id = MD5(reference_date || target_horizon_date || asset_id || model_version)`.

---

## 🏛️ Enterprise BI Serving Layer & Dimensional Modeling

To ensure optimal query performance, sub-second dashboard rendering, and governance, the Gold serving layer in **Snowflake** is structured following the **Kimball Dimensional Modeling (Star Schema)** methodology.

```text
                      ┌───────────────────────────────┐
                      │         DIM_CALENDAR          │
                      │ (PK: calendar_date)           │
                      │ • 4,018 rows (2020-2030)      │
                      │ • is_trading_day_b3 (BOOLEAN) │
                      │ • is_us_market_open (BOOLEAN) │
                      │ • year, quarter, month, week  │
                      └───────┬───────────────┬───────┘
                              │               │
             ┌────────────────┘               └────────────────┐
             │ (reference_date)                                │ (reference_date / target_horizon_date)
             ▼                                                 ▼
┌─────────────────────────────┐                   ┌─────────────────────────────┐
│      FACT_MARKET_DAILY      │                   │    FACT_MACRO_FORECASTS     │
│ (PK: reference_date,        │                   │ (PK: forecast_id)           │
│      asset_id)              │                   │ • reference_date (FK)       │
│ • close_price (DECIMAL)     │                   │ • target_horizon_date (FK)  │
│ • daily_return / log_return │                   │ • asset_id (FK)             │
│ • sma_7 / sma_30 / sma_200  │                   │ • horizon_days (SMALLINT)   │
│ • rolling_vol_30d/60d/90d   │                   │ • predicted_value (DECIMAL) │
│ • real_interest_rate (DEC)  │                   │ • lower_bound_95 (DECIMAL)  │
│ • selic_vs_us_spread (DEC)  │                   │ • upper_bound_95 (DECIMAL)  │
│ • ingestion_timestamp       │                   │ • model_version (VARCHAR)   │
└──────────────▲──────────────┘                   │ • created_at (TIMESTAMP)    │
               │                                  └──────────────▲──────────────┘
               │         ┌───────────────────────────────┐       │
               │         │           DIM_ASSET           │       │
               └─────────┤ (PK: asset_id)                ├───────┘
                         │ • ticker_or_code (VARCHAR)    │
                         │ • asset_name (VARCHAR)        │
                         │ • asset_class (FX/Rates/...)  │
                         │ • data_source (BCB/YFinance)  │
                         │ • quote_currency (BRL/USD/...)│
                         │ • is_active (BOOLEAN)         │
                         └───────────────────────────────┘
```

### 1. Conformed Dimensions & Facts

* **`DIM_CALENDAR` (Conformed & Role-Playing Dimension):** Centralized temporal anchor generated over 4,018 continuous days (`2020-01-01` to `2030-12-31`). Correctly incorporates Brazilian national holidays, banking closures (Carnival, Good Friday, Corpus Christi), and US NYSE/Nasdaq holidays. Acts as a dual role-playing dimension connecting both the scoring timestamp (`reference_date`) and the target horizon (`target_horizon_date`).
* **`DIM_ASSET` (Conformed Dimension - SCD Type 1):** Master asset repository with conformed metadata across seven tracked series:
  * `1: USDBRL=X` (FX / YAHOO_FINANCE / BRL)
  * `2: ^BVSP` (Equities / YAHOO_FINANCE / INDEX_POINTS)
  * `3: BTC-BRL` (Crypto / YAHOO_FINANCE / BRL)
  * `4: USD_PTAX` (FX / BCB_SGS / BRL)
  * `5: SELIC` (Rates / BCB_SGS / PERCENT_DAY)
  * `6: IPCA` (Inflation / BCB_SGS / PERCENT_MONTH)
  * `7: ^IRX` (Rates / YAHOO_FINANCE / PERCENT_PA)
* **`FACT_MARKET_DAILY` (Periodic Snapshot Fact):** Granular at `reference_date` $\times$ `asset_id`, recording verified market close values (`Decimal(18, 8)`), daily and log returns, moving averages (SMA 7, 30, 200), annualized rolling volatilities (30, 60, 90 days), Fisher real interest rates, and sovereign yield differentials.
* **`FACT_MACRO_FORECASTS` (ML Inference Fact):** Stores multi-horizon time-series predictions generated by the MLOps pipeline. Keyed by surrogate `forecast_id` with foreign keys to `DIM_ASSET` and `DIM_CALENDAR`, capturing projected values, conformal 95% confidence bounds (`lower_bound_95`, `upper_bound_95`), target horizons ($h=1 \dots 7$), and MLflow active model version tags.

### 2. Analytical Semantic Queries (`bi/queries/`)

Version-controlled SQL queries located in `bi/queries/` serve as the semantic consumption layer for analytics:
* **`01_macro_risk_real_rate_spread.sql`:** Analyzes the relationship between Brazil's Fisher real interest rate, sovereign rate differential (`selic_vs_us_spread`), and USD/BRL currency depreciation, evaluating monetary transmission lags.
* **`02_cross_asset_rolling_metrics.sql`:** Aggregates cross-asset risk indicators, annualized rolling volatility regimes, and moving average spreads across equities (`^BVSP`), crypto (`BTC-BRL`), and FX (`USDBRL=X`).
* **`03_forecast_vs_actual_hedging.sql`:** Joins multi-horizon predictions from `FACT_MACRO_FORECASTS` against realized market closes from `FACT_MARKET_DAILY`, calculating out-of-sample forecast errors ($MAE, RMSE, MAPE$), directional accuracy ratios, and 95% conformal interval coverage rates.
* **Visual Dashboards (`bi/dashboards/`):** Dedicated Power BI and Streamlit templates are scheduled for deployment following warehouse mart provisioning (`PLANNED ⚪`).

---

## 📂 Repository Structure

```text
.
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI: Ruff, Mypy, DagBag & Pytest with 80% coverage gate
├── airflow/
│   ├── dags/
│   │   ├── macroflow_ingestion_bronze.py   # Airflow DAG: parallel extractors, raw upload & Lakehouse pipeline
│   │   └── macroflow_ml_inference_batch.py  # Airflow DAG: D+1 batch forecasting & Snowflake sync trigger
│   └── plugins/                       # Airflow plugin directory
├── bi/
│   ├── dashboards/                    # Power BI templates / Streamlit analytical apps (PLANNED ⚪)
│   └── queries/                       # Versioned semantic layer queries for Snowflake BI
│       ├── 01_macro_risk_real_rate_spread.sql
│       ├── 02_cross_asset_rolling_metrics.sql
│       └── 03_forecast_vs_actual_hedging.sql
├── databricks/
│   ├── 01_bronze_ingestion.py         # Bronze Delta partition ingestion from raw NDJSON
│   ├── 02_silver_cleaning.py          # Silver conformed layer: Bronze incremental reads, dedup & Delta MERGE
│   ├── 03_gold_analytics.py           # Gold analytics: asset returns, annualization, Fisher real rate, sovereign spread
│   ├── 04_snowflake_sync.py           # Gold to Snowflake serving sync: surrogate key mapping & staged MERGE
│   └── ml/
│       ├── 01_feature_store.py        # FX feature engineering: lags, Wilder RSI, BB spread, multi-horizon targets
│       ├── 02_train_and_register.py   # Walk-forward CV, Direct Multi-Horizon LightGBM, CQR bounds & MLflow @champion
│       └── 03_batch_predict.py        # D+1..D+7 B3 trading day scoring, conformal bounds & Snowflake staged MERGE
├── snowflake/
│   ├── 01_ddl_setup.sql               # Authoritative infrastructure: warehouse, database, schema & RBAC roles
│   └── 02_dimensional_marts.sql       # Kimball Star Schema: 4,018-day DIM_CALENDAR, DIM_ASSET, and Fact tables
├── src/
│   ├── __init__.py
│   ├── extractors/                    # Edge extraction modules
│   │   ├── __init__.py
│   │   ├── airflow_helpers.py         # Databricks operator kwargs, script paths & failure alerting
│   │   ├── bcb.py                     # BCB SGS API extractor (SELIC 11, IPCA 433, USD_PTAX 1)
│   │   ├── storage.py                 # NDJSON writer & Databricks UC Volumes / DBFS uploader
│   │   └── yfinance_extractor.py      # Yahoo Finance extractor (USDBRL=X, ^BVSP, BTC-BRL, ^IRX)
│   ├── ml_utils/                      # MLOps quantitative modeling & inference utilities
│   │   ├── __init__.py
│   │   ├── features.py                # Wilder RSI, MA divergence, multi-horizon target builder & feature store
│   │   ├── inference.py               # B3 trading calendar, freshness validator, forecast schema & MERGE SQL
│   │   └── training.py                # Temporal splits, walk-forward CV, Direct LightGBM + CQR, benchmark gates
│   ├── schemas/                       # Shift-left runtime data contracts
│   │   ├── __init__.py
│   │   └── macro_models.py            # Pydantic v2 edge contracts (BCBRecord, MarketRecord) & Spark schemas
│   └── transforms/                    # Modular PySpark Lakehouse transformations
│       ├── __init__.py
│       ├── bronze.py                  # Raw NDJSON batch reading, metadata attachment & Delta table ingestion
│       ├── silver.py                  # Incremental Bronze reading, standardization, dedup & Silver MERGE
│       └── gold.py                    # Asset analytics, Fisher real rate, IPCA 40d lag & Gold MERGE
├── tests/
│   ├── test_contracts.py              # Pydantic v2 contracts, domain bounds, OHLC rules & schema getters
│   ├── test_ingestion.py              # BCB specs, YFinance extraction, NDJSON writing & Airflow helpers
│   ├── test_mlops.py                  # Wilder RSI, temporal splits, CQR bounds, promotion gates & B3 calendar
│   └── test_transforms.py             # Pure PySpark transformations: Bronze, Silver dedup/precision & Gold math
├── .env.example                       # Documented environment template for Docker Compose & credentials
├── docker-compose.yaml                # Local Apache Airflow deployment with built image & PostgreSQL
├── Dockerfile                         # Custom Airflow image installing src/ and pinned requirements
├── requirements.txt                   # Locked Python dependencies
├── ruff.toml                          # Static analysis and linting configuration
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

* **Docker & Docker Compose** (for running Airflow locally)
* **Python 3.11+**
* **Databricks Workspace** (Unity Catalog enabled recommended)
* **Snowflake Account** (with `SYSADMIN` and `SECURITYADMIN` privileges to run initial DDL)

### 1. Clone & Setup Repository

```bash
git clone https://github.com/hugoargerio-glitch/MacroFlow-Lakehouse.git
cd MacroFlow-Lakehouse
```

### 2. Environment Configuration

Copy `.env.example` to `.env` and populate real credentials:

```bash
cp .env.example .env
```

Key environment variables in `.env`:

```env
# Airflow Image & Secret Key
AIRFLOW_IMAGE_NAME=macroflow-airflow:latest
AIRFLOW_UID=50000
FERNET_KEY=generate_with_cryptography_fernet

# Airflow PostgreSQL Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=airflow
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=your_secure_password

# Databricks Workspace & Storage Paths
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_pat
DATABRICKS_CLUSTER_ID=your_cluster_id
DATABRICKS_RAW_BASE_PATH=/Volumes/macroflow/bronze/raw_data
DATABRICKS_SCRIPTS_BASE_PATH=/Volumes/macroflow/artifacts/scripts/databricks
AIRFLOW_CONN_DATABRICKS_DEFAULT=databricks://?host=https%3A%2F%2Fyour-workspace.cloud.databricks.com&token=your_databricks_pat

# Snowflake Serving Warehouse (Databricks reads via dbutils.secrets in production)
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_DATABASE=MACROFLOW_DW
SNOWFLAKE_SCHEMA=GOLD
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=MACROFLOW_ETL_ROLE

ENVIRONMENT=development
LOG_LEVEL=INFO
```

### 3. Run Apache Airflow Locally

Build the custom Airflow image (which packages `src/` and project dependencies) and start containers:

```bash
docker compose build
docker compose up -d
```

Access the Airflow Web UI at `http://localhost:8080` (credentials configured in `.env`).

### 4. Execute Code Quality & Test Suite

Run static analysis, type checking, and automated unit tests:

```bash
# 1. Static code analysis and formatting checks
ruff check .
ruff format --check .

# 2. Static type checking
mypy src/ airflow/dags/ --ignore-missing-imports

# 3. Unit test suite with coverage enforcement (>= 80%)
pytest tests/ -v --tb=short --cov=src --cov-fail-under=80
```

---

## 📈 Updated Engineering Decisions & Trade-Offs

| Decision | Chosen Approach | Alternative Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Forecasting Model Architecture** | **Direct Multi-Horizon LightGBM ($h=1..7$)** | Recursive 1-step autoregressive model | Recursive loops compound single-step errors exponentially and require simulating or freezing future exogenous variables. Direct multi-horizon models train specialized estimators per horizon, preserving accuracy across the full projection path. |
| **Prediction Uncertainty** | **Conformalized Quantile Regression (CQR)** | Static standard deviation bands ($\pm 1.96 \sigma$) | Static multipliers assume constant Gaussian residual distributions. CQR combines quantile regression with distribution-free conformal calibration, delivering mathematically valid 95% coverage across volatile FX regimes. |
| **Real Interest Rate Formula** | **Fisher Equation with 40-Day IPCA Lag** | Direct rate subtraction ($\text{SELIC} - \text{IPCA}$) | Simple subtraction violates economic reality and conflates daily and monthly frequencies. The Fisher equation properly compounds annualized rates, while shifting monthly IPCA by 40 days eliminates lookahead bias. |
| **Lakehouse Storage & Deduplication** | **Delta Lake + Ingestion Watermarks** | Append-only raw JSON scanning | Scanning raw folders quadratically inflates I/O on every run. Delta Lake enables ACID transactions, Liquid Clustering, and deterministic deduplication ordered by ingestion timestamp. |
| **Warehouse Access Control** | **Dual Role RBAC (`ETL_ROLE` vs `BI_READER`)** | Single `ACCOUNTADMIN` user | Enforces least-privilege security by completely isolating write/DDL privileges required for pipeline jobs from read-only analytical access required for BI queries and dashboards. |
| **Data Contracts** | **Pydantic v2 (Edge) + PySpark `StructType` (Lake)** | Schema-on-Read | Ingesting unvalidated JSON risks pipeline crashes in downstream compute clusters. Shift-left validation at the edge catches malformed API payloads before spawning expensive Spark clusters. |

---

## 👤 Author

### Hugo Campos

* **LinkedIn:** [linkedin.com/in/Hugo-Campos](https://www.linkedin.com/in/hugo-campos-b2a678273/)
* **GitHub:** [github.com/hugoargerio-glitch](https://github.com/hugoargerio-glitch?tab=repositories)
