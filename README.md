# 🌐 MacroFlow: Enterprise Macroeconomic & FX Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark%20%7C%20Delta%20Lake-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-Automated%20CI-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)

An automated, production-grade end-to-end data pipeline designed to ingest, validate, transform, and serve macroeconomic indicators and currency exchange rates.

The platform integrates public APIs from the **Central Bank of Brazil (BCB/SGS)** and financial APIs (**Yahoo Finance**), enforcing data contracts at ingestion with **Pydantic**, processing data through a **Medallion Architecture (Bronze, Silver, Gold)** on **Databricks (PySpark + Delta Lake)**, orchestrating tasks with **Apache Airflow**, and publishing analytical data marts to **Snowflake**.

---

## 🚦 Project Implementation Status

| Milestone / Layer | Description | Engine / Tech | Status |
| :--- | :--- | :--- | :--- |
| **01. Ingestion & Edge Contracts** | Multi-source extraction (BCB + YFinance), dynamic sliding window, Pydantic schema validation & Hive-partitioned raw JSON sink. | Apache Airflow, Python 3.11, Pydantic | `DONE` ✅ |
| **02. Warehouse Provisioning** | DDL setup for analytical marts, auto-suspending virtual warehouses & RBAC security setup. | Snowflake SQL | `DONE` ✅ |
| **03. Silver Layer (Lakehouse)** | Strict schema casting, business deduplication, trading calendar forward-fill & idempotent Delta `MERGE INTO`. | Databricks, PySpark, Delta Lake | `IN PROGRESS` 🟡 |
| **04. Gold Layer (Analytics)** | Rolling volatility (30/60/90d), exponential moving averages (SMA/EMA) & macro indicator correlation models. | Databricks, PySpark SQL | `PLANNED` ⚪ |
| **05. Serving Layer** | Pipeline sync from Databricks to Snowflake analytical tables and optimized BI views. | Snowflake Connector, S3 / DBFS | `PLANNED` ⚪ |
| **06. CI/CD & Observability** | Automated GitHub Actions workflow (Ruff linting, Pytest contract tests) & Airflow SLA alerting. | GitHub Actions, Pytest | `PLANNED` ⚪ |

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
             ┌───────────────────────────────────────────────┐
             │            DATABRICKS LAKEHOUSE               │
             │                                               │
             │ 🥉 BRONZE: Hive-Partitioned Raw JSON Logs     │
             │            (year=YYYY/month=MM/day=DD)        │
             │                     │                         │
             │ 🥈 SILVER: Cleaned, Typed & Imputed           │
             │            (PySpark ETL + Delta Lake MERGE)   │
             │                     │                         │
             │ 🥇 GOLD: Aggregated Analytics & Risk Metrics  │
             │          (Rolling Volatility, SMAs, Spreads)  │
             └───────────────────────┬───────────────────────┘
                                     │ (Snowflake Spark Connector)
                                     ▼
             ┌───────────────────────────────────────────────┐
             │              SNOWFLAKE WAREHOUSE              │
             │ • Database: MACROFLOW_DW | Schema: GOLD       │
             │ • Optimized Serving Layer for High-Load BI    │
             └───────────────────────────────────────────────┘

```

---

## 🎯 Key Architectural Highlights

* **Medallion Architecture:** Strict segregation of concerns across Bronze (raw historical log), Silver (cleansed, deduplicated, single source of truth), and Gold (business KPIs and analytical aggregates).


* **Shift-Left Data Contracts:** Data contract validation at ingestion using **Pydantic** to reject malformed API responses and schema drift before reaching the lakehouse compute clusters.


* **ACID Compliance & Idempotency:** Powered by **Delta Lake**, enabling schema enforcement, time travel, and transactional upserts (`MERGE INTO`) preventing duplicated records on backfills.


* **Decoupled Architecture:** Workload isolation between batch heavy computing (**Databricks / PySpark**) and high-concurrency analytical querying (**Snowflake**).


* **Enterprise Orchestration:** DAG-driven workflow with **Apache Airflow**, implementing dynamic sliding windows, automated retries with exponential backoff, and SLA monitoring.



---

## 📂 Repository Structure

```text
.
├── .github/
│   └── workflows/
│       └── ci.yml                 # Automated linting (Ruff), type checking, and Pytest
├── airflow/
│   ├── dags/
│   │   ├── contracts/
│   │   │   └── macro_models.py    # Pydantic models for data validation
│   │   └── macroflow_ingestion_bronze.py # Multi-source extraction DAG
│   └── plugins/
├── databricks/
│   ├── 01_bronze_ingestion.py     # API extraction, contract check & raw Delta sink
│   ├── 02_silver_cleaning.py      # PySpark schema casting, dedup & Merge Upsert
│   └── 03_gold_analytics.py       # Window functions (Moving Avg, Volatility, Spread)
├── snowflake/
│   ├── 01_ddl_setup.sql           # Database, schema, warehouse and table DDL
│   └── 02_analytics_views.sql     # Materialized views for macroeconomic KPIs
├── src/
│   ├── __init__.py
│   ├── extractors/                # API wrapper classes (BCB, Yahoo Finance)
│   └── schemas/                   # Pydantic data models enforcing data contracts
├── tests/
│   ├── test_contracts.py          # Schema validation unit tests
│   └── test_transforms.py         # PySpark transformation logic unit tests
├── docker-compose.yaml            # Local Apache Airflow deployment configuration
├── requirements.txt               # Python dependencies
└── README.md

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
-- DDL Sample: Fact Market Table
CREATE TABLE IF NOT EXISTS MACROFLOW_DW.GOLD.FACT_MARKET_DAILY (
    reference_date DATE NOT NULL,
    ticker_or_code VARCHAR(30) NOT NULL,
    asset_name VARCHAR(100),
    close_price NUMBER(18, 4),
    daily_return NUMBER(10, 6),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (reference_date, ticker_or_code)
);

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

## 📈 Engineering Decisions & Trade-Offs

| Decision | Chosen Approach | Alternative Considered | Rationale |
| :--- | :--- | :--- | :--- |
| **Orchestration** | **Apache Airflow** | Databricks Workflows | Airflow provides enterprise-grade multi-platform orchestration across Databricks, APIs, and Snowflake. |
| **Storage Engine** | **Delta Lake** | Raw Parquet files | Delta Lake guarantees ACID transactions, Time Travel auditing, and native `MERGE` upsert capabilities. |
| **Data Contracts** | **Pydantic (Edge)** | Spark Schema-on-Read | Pydantic blocks malformed payloads at the ingestion edge before incurring Spark cluster compute costs. |
| **Serving Layer** | **Snowflake** | Databricks SQL Serverless | Isolates high-concurrency analytical queries from heavy data transformation workloads. |

---

## 👤 Author

### Hugo Campos

#### LinkedIn: [linkedin.com/in/Hugo-Campos](https://www.linkedin.com/in/hugo-campos-b2a678273/)

#### GitHub: [github.com/hugoargerio-glitch](https://github.com/hugoargerio-glitch?tab=repositories)
