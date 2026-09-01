# 🌐 MacroFlow: Enterprise Macroeconomic & FX Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark%20%7C%20Delta%20Lake-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-Automated%20CI-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![MLflow](https://img.shields.io/badge/MLflow-Experiment%20Tracking%20%26%20Registry-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)](https://mlflow.org/)
[![LightGBM](https://img.shields.io/badge/LightGBM-Time--Series%20Forecasting-FFA100?style=for-the-badge&logo=scikitlearn&logoColor=white)](https://lightgbm.readthedocs.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dimensional%20Analytics-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

An automated, enterprise-grade data platform engineered to ingest, validate, model, forecast, and serve macroeconomic indicators and currency exchange rates.

The platform integrates public APIs from the **Central Bank of Brazil (BCB/SGS)** and financial APIs (**Yahoo Finance**), enforcing data contracts at ingestion with **Pydantic**, processing data through a **Medallion Architecture (Bronze, Silver, Gold)** on **Databricks (PySpark + Delta Lake)**, orchestrating workflows with **Apache Airflow**, executing automated **MLOps batch forecasting pipelines (MLflow + LightGBM)**, and serving Kimball-modeled dimensional data marts to **Snowflake** for executive **Business Intelligence (BI)** dashboards.

---

## 🚦 Project Implementation Status

| Milestone / Layer | Description | Engine / Tech | Status |
| :--- | :--- | :--- | :--- |
| **01. Ingestion & Edge Contracts** | Multi-source extraction (BCB + YFinance), dynamic sliding window, Pydantic schema validation & Hive-partitioned raw JSON sink. | Apache Airflow, Python 3.11, Pydantic | `DONE` ✅ |
| **02. Warehouse Provisioning** | DDL setup for analytical marts, auto-suspending virtual warehouses & RBAC security setup. | Snowflake SQL | `DONE` ✅ |
| **03. Silver Layer (Lakehouse)** | Strict schema casting, business deduplication, trading calendar forward-fill & idempotent Delta `MERGE INTO`. | Databricks, PySpark, Delta Lake | `IN PROGRESS` 🟡 |
| **04. Gold Layer (Feature Store & Marts)** | Rolling volatility (30/60/90d), SMA/EMA indicators, time-lagged feature matrices & macro spread analytics. | Databricks, PySpark SQL, Delta Lake | `PLANNED` ⚪ |
| **05. Serving Layer (Kimball Marts)** | Pipeline sync from Databricks to Snowflake dimensional star-schema (`FACT_MARKET_DAILY`, `DIM_CALENDAR`, `DIM_ASSET`). | Snowflake Spark Connector, DBFS | `PLANNED` ⚪ |
| **06. MLOps Batch Inference Engine** | Automated feature retrieval, MLflow experiment tracking/registry, LightGBM multi-horizon FX forecasting & prediction sinking. | MLflow, LightGBM, Apache Airflow | `PLANNED` ⚪ |
| **07. Executive BI & Observability** | Macro risk dashboards (Power BI / Streamlit), data drift detection, SLA monitoring & CI/CD workflow testing. | Power BI, GitHub Actions, Pytest | `PLANNED` ⚪ |

---

## 🎯 Key Architectural Highlights

* **Medallion Architecture:** Strict segregation of concerns across Bronze (raw historical log), Silver (cleansed, deduplicated, single source of truth), and Gold (business KPIs and analytical aggregates).


* **Shift-Left Data Contracts:** Data contract validation at ingestion using **Pydantic** to reject malformed API responses and schema drift before reaching the lakehouse compute clusters.


* **ACID Compliance & Idempotency:** Powered by **Delta Lake**, enabling schema enforcement, time travel, and transactional upserts (`MERGE INTO`) preventing duplicated records on backfills.


* **Decoupled Architecture:** Workload isolation between batch heavy computing (**Databricks / PySpark**) and high-concurrency analytical querying (**Snowflake**).


* **Enterprise Orchestration:** DAG-driven workflow with **Apache Airflow**, implementing dynamic sliding windows, automated retries with exponential backoff, and SLA monitoring.



---


## 🏗️ System Architecture

```
                              [DATA SOURCES]
             ┌───────────────────────┴───────────────────────┐
             │                                               │
      [BCB SGS API]                                   [Yahoo Finance API]
 (IPCA, Selic, USD PTAX)                             (USD/BRL, IBOV, BTC)
             │                                               │
             └───────────────────────┬───────────────────────┘
                                     │
                                     ▼
                    [ AIRFLOW EDGE INGESTION LAYER ]
                    • Shift-Left Contract Validation (Pydantic)
                    • Dynamic Date-Windowing & Retry Policies
                                     │
                                     ▼
        ┌────────────────────────────┬──────────────────────────────┐
        │                    DATABRICKS LAKEHOUSE                   │
        │                                                           │
        │ 🥉 BRONZE: Hive-Partitioned Raw JSON Logs                 │
        │            (year=YYYY/month=MM/day=DD)                    │
        │                             │                             │
        │ 🥈 SILVER: Cleaned, Typed & Imputed                       │
        │            (PySpark ETL + Delta Lake MERGE)               │
        │                             │                             │
        │ 🥇 GOLD: Aggregated Analytics & Risk Metrics              │
        │          (Rolling Volatility, SMAs, Spreads)              │
        └──────────────┬────────────────────────────┬───────────────┘
                       │                            │
             [ FEATURE EXTRACTION ]          [ SNOWFLAKE SYNC ]
                       │                            │
                       ▼                            ▼
        ┌───────────────────────────────┐ ┌───────────────────────────────┐
        │         MLOPS ENGINE          │ │      SNOWFLAKE WAREHOUSE      │
        │ • Feature Store Ingestion     │ │                               │
        │ • MLflow Experiment Tracking  │ │ 🏛️ KIMBALL DIMENSIONAL MARTS  │
        │ • Model Registry & Versioning │ │ • DIM_CALENDAR                │
        │ • Airflow Batch Inference DAG │ │ • DIM_ASSET                   │
        │ • Forecast Residual Sinks     │ │ • FACT_MARKET_DAILY           │
        └───────────────┬───────────────┘ │ • FACT_MACRO_FORECASTS        │
                        │ (Predictions)   └───────────────┬───────────────┘
                        └─────────────────────────────────┘
                                        │
                                        ▼
                        ┌─────────────────────────────────┐
                        │   BI & ANALYTICAL CONSUMPTION   │
                        │ • Macroeconomic Risk Dashboard  │
                        │ • FX Volatility & Forecast BI   │
                        └─────────────────────────────────┘
```

---


## 📊 Medallion Architecture Breakdown

### 🥉 Bronze Layer (Raw Ingestion)

* **Sources:**
* Central Bank of Brazil (SGS API): Series `433` (Monthly IPCA), `11` (Daily Selic), `1` (USD PTAX).


* Yahoo Finance API: Daily close, open, high, low, volume for `USDBRL=X`, `^BVSP`, `BTC-BRL`.




* **Format:** Raw JSON logs partitioned by ingestion date (`year=YYYY/month=MM/day=DD`).


* **Governance:** Append-only log preserving original source payloads for auditability.



### 🥈 Silver Layer (Cleaned & Conformed)

* **Schema Enforcement:** Strict type casting using PySpark (`TimestampType`, `DecimalType(18, 4)`).


* **Data Cleansing:** Imputation of missing market dates (holidays/weekends) via calendar dimension forward-fill.


* **Idempotency:** Delta Lake `MERGE INTO` operations against compound business keys (`reference_date` + `ticker_or_code`).



### 🥇 Gold Layer (Aggregations & Analytics)

* **Calculated Metrics:**
* **30/60/90-Day Rolling Volatility:** Dynamic standard deviation over daily returns.


* **Moving Averages (SMA/EMA):** Short and long-term trend lines (7, 30, and 200 days).


* **Macro Correlation:** Macroeconomic spread tracking FX oscillation against Selic/IPCA shifts.

---


## ❄️ Snowflake Data Warehouse Setup

Analytical models are provisioned in Snowflake with automatic credit suspension (`AUTO_SUSPEND = 60`).

```sql
CREATE TABLE IF NOT EXISTS MACROFLOW_DW.GOLD.FACT_MARKET_DAILY (
    reference_date DATE NOT NULL,
    asset_id INTEGER NOT NULL,
    close_price NUMBER(18, 4),
    log_return NUMBER(10, 6),
    rolling_vol_30d NUMBER(10, 6),
    selic_vs_us_spread NUMBER(10, 6),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (reference_date, asset_id),
    CONSTRAINT fk_asset FOREIGN KEY (asset_id) REFERENCES MACROFLOW_DW.GOLD.DIM_ASSET(asset_id),
    CONSTRAINT fk_calendar FOREIGN KEY (reference_date) REFERENCES MACROFLOW_DW.GOLD.DIM_CALENDAR(calendar_date)
);

```

---

## 🤖 Production MLOps & Time-Series Forecasting Engine

Rather than treating Machine Learning as an isolated notebook experiment, MacroFlow operationalizes forecasting through a robust **MLOps lifecycle**. The Data Engineering pipeline automates the transformation of curated Gold tables into a point-in-time consistent **Feature Store**, executes distributed training, tracks experiment lineage via **MLflow**, and automates scheduled batch inference workflows orchestrated by **Apache Airflow**.

```
[ Databricks Gold Layer ]
           │
           ▼
[ 1. Feature Store Engine ]   ──▶ Time-Lag Generation ($t-1 \dots t-30$), Rolling Volatility, Spreads
           │
           ▼
[ 2. MLflow Tracking Hub  ]   ──▶ Hyperparameter Optimization, Artifact Logging & $RMSE$/$MAPE$ Telemetry
           │
           ▼
[ 3. Production Registry  ]   ──▶ Semantic Versioning (Staging ➔ Production) based on Benchmark Gates
           │
           ▼
[ 4. Airflow Inference DAG]   ──▶ Daily $D+1$ Batch Scoring Job execution via Databricks Submit Run Operator
           │
           ▼
[ 5. Snowflake Sink Mart  ]   ──▶ Ingestion into FACT_MACRO_FORECASTS for Executive BI Consumption
          
```
### 1. Feature Engineering & Feature Store
The automated feature generation module transforms raw financial time series into a supervised learning matrix, enforcing **as-of temporal joins** to prevent data leakage (lookahead bias):
* **Temporal Lags:** Autoregressive feature matrices capturing multi-interval momentum ($t-1, t-3, t-5, t-15, t-30$).
* **Statistical Moments:** Rolling kurtosis, dynamic volatility windows (14-day RSI, Bollinger Band spreads), and moving average divergence.
* **Macro Exogenous Drivers:** Real-time yield differential between SELIC policy shifts and US Dollar liquidity.

### 2. Model Architecture & Experiment Tracking (MLflow)
* **Forecasting Algorithm:** **LightGBM Regressor** customized for recursive time-series forecasting, selected for its superior handling of non-linear macroeconomic shifts, computational efficiency, and low memory footprint in distributed Spark environments.
* **Experiment Lineage:** Every training execution automatically logs:
  * **Parameters:** Learning rate, number of leaves, objective function, regularization hyperparameters ($L1/L2$).
  * **Evaluation Metrics:** Out-of-time validation metrics ($RMSE$, $MAE$, $MAPE$, Directional Accuracy Ratio).
  * **Artifacts:** Serialized model binaries (`.booster`), feature importance plots, and SHAP summary charts.

### 3. Automated Batch Scoring Pipeline
1. **Trigger:** The daily ingestion DAG completes Gold layer transformations and emits an Airflow execution trigger to the `macroflow_ml_inference_batch` DAG.
2. **Model Loading:** The inference worker queries the MLflow Model Registry via URI (`models:/MacroFlow_USDBRL_Forecaster/Production`) to load the active champion model.
3. **Inference Execution:** Generates multi-step ahead predictions ($t+1 \dots t+7$) with calculated prediction intervals ($95\%$ upper/lower confidence bounds).
4. **Idempotent Loading:** Predictions and residual logs are written directly to Snowflake's `FACT_MACRO_FORECASTS` table using transactional merge operations.

----

## 🏛️ Enterprise BI Serving Layer & Dimensional Modeling

To ensure optimal query performance, sub-second dashboard rendering, and governance, the Gold serving layer in **Snowflake** is structured following the **Kimball Dimensional Modeling (Star Schema)** methodology. This decouples analytical processing from operational transformations and provides an intuitive structure for Business Intelligence analysts.

```
                      ┌───────────────────────────────┐
                      │         DIM_CALENDAR          │
                      │ (PK: calendar_date)           │
                      │ • is_trading_day_b3 (BOOLEAN) │
                      │ • is_us_market_open (BOOLEAN) │
                      │ • year, quarter, month, week  │
                      └───────┬───────────────┬───────┘
                              │               │
             ┌────────────────┘               └────────────────┐
             │ (reference_date)                                │ (target_date)
             ▼                                                 ▼
┌─────────────────────────────┐                   ┌─────────────────────────────┐
│      FACT_MARKET_DAILY      │                   │    FACT_MACRO_FORECASTS     │
│ (PK: reference_date,        │                   │ (PK: forecast_id)           │
│      asset_id)              │                   │ • reference_date (FK)       │
│ • close_price (DECIMAL)     │                   │ • target_horizon_date (FK)  │
│ • log_return (DECIMAL)      │                   │ • asset_id (FK)             │
│ • rolling_vol_30d (DECIMAL) │                   │ • predicted_value (DECIMAL) │
│ • selic_vs_us_spread (DEC)  │                   │ • lower_bound_95 (DECIMAL)  │
└──────────────▲──────────────┘                   │ • upper_bound_95 (DECIMAL)  │
               │                                  │ • model_version (VARCHAR)   │
               │                                  └──────────────▲──────────────┘
               │         ┌───────────────────────────────┐       │
               │         │           DIM_ASSET           │       │
               └─────────┤ (PK: asset_id)                ├───────┘
                         │ • ticker_or_code (VARCHAR)    │
                         │ • asset_class (FX/Macro/Index)│
                         │ • data_source (BCB/YFinance)  │
                         │ • quote_currency (BRL/USD)    │
                         └───────────────────────────────┘
```

### 1. Data Mart Architecture (Kimball Star Schema)

* **`DIM_CALENDAR` (Conformed & Role-Playing Dimension):** Centralized temporal anchor resolving Brazilian (B3/ANBIMA) and US financial calendars, holiday schedules, and trading day flags. Acts as a dual role-playing dimension connecting both the scoring timestamp (`reference_date`) and prediction horizon (`target_horizon_date`).
* **`DIM_ASSET` (Conformed Dimension - SCD Type 1/2):** Master asset repository providing cross-cutting categorization across equities (`^BVSP`), foreign exchange (`USDBRL=X`, `USD PTAX`), crypto assets (`BTC-BRL`), and benchmark interest rates (`SELIC`, `IPCA`).
* **`FACT_MARKET_DAILY` (Periodic Snapshot Fact):** Granular at `reference_date` $\times$ `asset_id`, recording verified market close values, log returns, exponential moving averages (EMA 21/50/200), rolling volatility windows (30/60/90d), and sovereign monetary spreads ($Selic - IPCA$).
* **`FACT_MACRO_FORECASTS` (ML Inference Fact):** Stores multi-horizon time-series predictions generated by the MLOps pipeline. Keyed by surrogate `forecast_id` with foreign keys to `DIM_ASSET` and `DIM_CALENDAR`, capturing projected values, $95\%$ prediction intervals (upper/lower bounds), actual vs. predicted residuals, and MLflow active model version tags.

### 2. Business Intelligence & Analytics Dashboards

* **Macro Risk & Currency Volatility Monitor:**
* **Real Interest Rate vs. FX Depreciation:** Dynamic scatter plots and trendlines tracking the correlation between Brazil's real interest spread ($Selic - IPCA$) and USD/BRL currency oscillation.
* **Cross-Asset Rolling Correlation Matrix:** Heatmap matrix evaluating dynamic 30/60/90-day rolling correlation shifts among FX, Commodities, and Crypto during monetary policy transitions.


* **Executive Forecasting & Hedging Hub:**
* **Multi-Step FX Horizon Projection:** Real-time visualization of $t+1 \dots t+7$ forecasts paired with $95\%$ confidence fan charts to assist corporate treasury and cash-flow hedging decisions.
* **MLOps Drift & Model Observability:** Performance telemetry dashboard monitoring prediction error drift ($MAE$, $RMSE$, $MAPE$) across batch training runs and tracking model version promotions.

---

 
## 📂 Repository Structure


```text
.
├── .github/
│   └── workflows/
│       └── ci.yml                     # Automated linting (Ruff), type checking, and Pytest
├── airflow/
│   ├── dags/
│   │   ├── contracts/
│   │   │   └── macro_models.py        # Pydantic models for data validation
│   │   ├── macroflow_ingestion_bronze.py
│   │   ├── macroflow_lakehouse_transform.py
│   │   └── macroflow_ml_inference_batch.py # Automated D+1 batch forecasting DAG
│   └── plugins/
├── bi/
│   ├── dashboards/                    # Power BI templates / Streamlit analytical apps
│   └── queries/                       # Optimized semantic layer queries for BI
├── databricks/
│   ├── 01_bronze_ingestion.py         # API extraction, contract check & raw Delta sink
│   ├── 02_silver_cleaning.py          # PySpark schema casting, dedup & Merge Upsert
│   ├── 03_gold_analytics.py           # Window functions (Moving Avg, Volatility, Spread)
│   └── ml/
│       ├── 01_feature_store.py        # Time-series feature engineering (lags, rolling stats)
│       ├── 02_train_and_register.py   # Model training, hyperparameter tuning & MLflow logging
│       └── 03_batch_predict.py        # Batch scoring engine loading production model artifact
├── snowflake/
│   ├── 01_ddl_setup.sql               # Database, schema, warehouse and RBAC DDL
│   ├── 02_dimensional_marts.sql       # Kimball Star Schema (Dimensions & Facts)
│   └── 03_analytics_views.sql         # Optimized views for reporting and feature serving
├── src/
│   ├── __init__.py
│   ├── extractors/                    # API wrapper classes (BCB, Yahoo Finance)
│   ├── ml_utils/                      # Backtesting, evaluation metrics (RMSE/MAPE), MLflow helpers
│   └── schemas/                       # Pydantic data models enforcing data contracts
├── tests/
│   ├── test_contracts.py              # Schema validation unit tests
│   ├── test_ml_features.py            # Feature engineering transformation tests
│   └── test_transforms.py             # PySpark transformation logic unit tests
├── docker-compose.yaml                # Local Apache Airflow deployment configuration
├── requirements.txt                   # Python dependencies
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

* **Docker & Docker Compose** (for running Airflow locally)


* **Python 3.11+**

* **Databricks Workspace** (Community or Enterprise)


* **Snowflake Account**


### 1. Clone & Setup Repository

```bash
git clone https://github.com/hugoargerio-glitch/MacroFlow-Lakehouse.git
cd MacroFlow-Lakehouse

```

### 2. Environment Configuration

Create a `.env` file in the project root:

```env
# Airflow Image
AIRFLOW_IMAGE_NAME=apache/airflow:2.8.1-python3.11
_PIP_ADDITIONAL_REQUIREMENTS=pydantic yfinance requests

# Databricks Credentials
DATABRICKS_HOST=https://<your-databricks-instance>.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_token
DATABRICKS_CLUSTER_ID=your_cluster_id

# Snowflake Credentials
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=MACROFLOW_DW
SNOWFLAKE_SCHEMA=GOLD
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

```

### 3. Run Apache Airflow

```bash
docker compose up -d

```

Access the Airflow Web UI at `http://localhost:8080` (Default credentials: `airflow`/`airflow`).

### 4. Run Unit Tests

```bash
pytest tests/ -v

```

---


## 📈 Updated Engineering Decisions & Trade-Offs


| Decision | Chosen Approach | Alternative Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Orchestration** | **Apache Airflow** | Databricks Workflows / Cron | Multi-platform orchestration capability across external APIs, Spark clusters, ML inference jobs, and Snowflake warehouses. |
| **Storage Engine** | **Delta Lake** | Raw Parquet / Hive Tables | Guarantees ACID transactions, point-in-time Time Travel auditing, and high-performance `MERGE` upserts. |
| **Data Contracts** | **Pydantic (Edge)** | Spark Schema-on-Read | Rejects corrupt or schema-drifted payloads before spawning compute clusters, eliminating wasted cloud compute costs. |
| **Warehouse Schema** | **Kimball Star Schema** | One Big Table (OBT) | Preserves semantic clarity, reduces storage redundancy, and simplifies slice-and-dice aggregations across BI tools. |
| **ML Lifecycle** | **MLflow + Batch DAG** | Ad-hoc Python Scripts | Provides centralized experiment lineage, reproducible model versioning, and zero-downtime model promotion. |
| **Inference Strategy** | **Scheduled Batch Scoring** | Real-Time REST Microservice | Macroeconomic indicators publish on daily/monthly schedules; batch scoring aligns compute cost directly with data arrival frequency. |

---

## 👤 Author

### Hugo Campos

#### LinkedIn: [linkedin.com/in/Hugo-Campos](https://www.linkedin.com/in/hugo-campos-b2a678273/)

#### GitHub: [github.com/hugoargerio-glitch](https://github.com/hugoargerio-glitch?tab=repositories)
