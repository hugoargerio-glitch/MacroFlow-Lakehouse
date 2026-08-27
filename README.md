# 🌐 MacroFlow: Enterprise Macroeconomic & FX Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark%20%7C%20Delta%20Lake-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-Automated%20CI-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)

An automated, production-grade end-to-end data pipeline designed to ingest, validate, transform, and serve macroeconomic indicators and currency exchange rates. 

The platform integrates public APIs from the **Central Bank of Brazil (BCB/SGS)** and financial APIs (**Yahoo Finance / CoinGecko**), processing data through a **Medallion Architecture (Bronze, Silver, Gold)** on **Databricks (PySpark + Delta Lake)**, orchestrating tasks with **Apache Airflow**, and publishing analytical data marts to **Snowflake**.

---

## 🏗️ System Architecture


```
                              [DATA SOURCES]
             ┌───────────────────────┴───────────────────────┐
             │                                               │
      [BCB SGS API]                                  [Yahoo Finance API]
 (IPCA, Selic, USD PTAX)                             (USD/BRL, IBOV, BTC)
             │                                               │
             └───────────────────────┬───────────────────────┘
                                     │  (Pydantic Schema Validation)
                                     ▼
             ┌───────────────────────────────────────────────┐
             │            DATABRICKS LAKEHOUSE               │
             │                                               │
             │  🥉 BRONZE: Raw JSON Daily Ingestion (Delta)  │
             │                     │                         │
             │  🥈 SILVER: Cleaned, Typed & Deduplicated     │
             │             (PySpark ETL + Delta Merge)       │
             │                     │                         │
             │  🥇 GOLD: Aggregations & Metrics             │
             │           (30d Volatility, Moving Averages)   │
             └───────────────────────┬───────────────────────┘
                                     │  (Snowflake Connector / S3)
                                     ▼
             ┌───────────────────────────────────────────────┐
             │             SNOWFLAKE WAREHOUSE               │
             │  • Serving Layer & Analytical Data Marts      │
             │  • Optimized SQL Views for BI / Analytics     │
             └───────────────────────────────────────────────┘
                                     ▲
                                     │ (Trigger & Monitor)
             ┌───────────────────────────────────────────────┐
             │                APACHE AIRFLOW                 │
             │  • Containerized Orchestrator (Docker)        │
             │  • Dependency Management, Retries & SLAs      │
             └───────────────────────────────────────────────┘

```


---

## 🎯 Key Architectural Highlights

* **Medallion Architecture:** Strict segregation of concerns across Bronze (raw historical log), Silver (cleansed, deduplicated, single source of truth), and Gold (business KPIs and analytical aggregates).
* **Robust Data Contracts:** Data contract validation at ingestion using **Pydantic** to reject malformed API responses and schema drift before reaching the lakehouse.
* **ACID Compliance & Time Travel:** Powered by **Delta Lake**, enabling schema enforcement, partition pruning, and transactional upserts (`MERGE INTO`).
* **Decoupled Architecture:** Workload isolation between batch heavy computing (**Databricks / Spark**) and high-concurrency analytical querying (**Snowflake**).
* **Enterprise Orchestration:** DAG-driven workflow with **Apache Airflow**, implementing idempotent tasks, dynamic backfills, automated retries, and failure alerts.

---

## 📂 Repository Structure

```text
.
├── .github/
│   └── workflows/
│       └── ci.yml               # Automated linting (Ruff), type checking, and Pytest
├── airflow/
│   ├── dags/
│   │   └── macro_pipeline_dag.py # Airflow DAG orchestrating Databricks & Snowflake
│   └── plugins/
├── databricks/
│   ├── 01_bronze_ingestion.py   # API extraction, contract check & raw Delta sink
│   ├── 02_silver_cleaning.py    # PySpark schema casting, dedup & Merge Upsert
│   └── 03_gold_analytics.py     # Window functions (Moving Avg, Volatility, Spread)
├── snowflake/
│   ├── 01_ddl_setup.sql         # Database, schema, role, and table definitions
│   └── 02_analytics_views.sql   # Materialized views for macroeconomic KPIs
├── src/
│   ├── __init__.py
│   ├── extractors/              # API wrapper classes (BCB, Yahoo Finance)
│   └── schemas/                 # Pydantic data models enforcing data contracts
├── tests/
│   ├── test_contracts.py        # Schema validation unit tests
│   └── test_transforms.py       # PySpark transformation logic unit tests
├── docker-compose.yaml          # Local Apache Airflow deployment configuration
├── requirements.txt             # Python dependencies
└── README.md

```

---

## 📊 Medallion Architecture Breakdown

### 🥉 Bronze Layer (Raw Ingestion)

* **Sources:**
* Central Bank of Brazil (SGS API): Series `433` (IPCA), `11` (Selic Rate), `1` (USD PTAX).
* Yahoo Finance API: Daily close, volume, high/low for `USDBRL=X`, `^BVSP`, `BTC-BRL`.


* **Format:** Raw JSON written to Delta Lake, partitioned by ingestion date (`year=YYYY/month=MM/day=DD`).
* **Governance:** Append-only log preserving original source payloads.

### 🥈 Silver Layer (Cleaned & Conformed)

* **Schema Enforcement:** Strict type casting using PySpark (`TimestampType`, `Decimal(18, 4)`).
* **Data Cleansing:** Imputation of missing market dates (holidays/weekends) via calendar dimension forward-fill.
* **Idempotency:** Delta Lake `MERGE INTO` operations against compound business keys (`indicator_code` + `reference_date`) preventing duplicate records on pipeline reruns.

### 🥇 Gold Layer (Aggregations & Analytics)

* **Calculated Metrics:**
* **30/60/90-Day Rolling Volatility:** Dynamic standard deviation over daily returns.
* **Moving Averages (SMA/EMA):** Short and long-term trend lines (7, 30, and 200 days).
* **Macro Correlation:** Macroeconomic spread tracking the relationship between FX oscillation and Selic/IPCA shifts.



---

## ❄️ Snowflake Data Warehouse Setup

The processed Gold data is loaded into Snowflake to power analytical dashboards and ad-hoc reporting.

```sql
-- Example Analytical View: FX Volatility vs. Inflation Rate
CREATE OR REPLACE VIEW VW_MACRO_INDICATORS_CORRELATION AS
SELECT 
    f.REFERENCE_DATE,
    f.USD_CLOSING_PRICE,
    f.USD_VOLATILITY_30D,
    s.SELIC_ANNUALIZED_RATE,
    i.IPCA_MONTHLY_RATE
FROM GOLD_CURRENCY_METRICS f
LEFT JOIN GOLD_SELIC_INDICATORS s 
    ON f.REFERENCE_DATE = s.REFERENCE_DATE
LEFT JOIN GOLD_IPCA_INDICATORS i 
    ON DATE_TRUNC('month', f.REFERENCE_DATE) = i.REFERENCE_MONTH
ORDER BY f.REFERENCE_DATE DESC;

```

---

## 🚀 Getting Started

### Prerequisites

* **Docker & Docker Compose** (for running Airflow locally)
* **Python 3.11+**
* **Databricks Community or Enterprise Workspace** (configured with Git Folders)
* **Snowflake Account** (Free Trial or Standard Edition)

### 1. Clone & Configure the Repository

```bash
git clone [https://github.com/](https://github.com/)<your-username>/MacroFlow-Lakehouse.git
cd MacroFlow-Lakehouse

```

### 2. Environment Variables

Create a `.env` file in the project root:

```env
DATABRICKS_HOST=https://<your-databricks-instance>.cloud.databricks.com
DATABRICKS_TOKEN=your_databricks_personal_access_token
DATABRICKS_CLUSTER_ID=your_cluster_id

SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=MACRO_ANALYTICS
SNOWFLAKE_SCHEMA=GOLD

```

### 3. Start Apache Airflow

```bash
docker-compose up -d

```

Access the Airflow Web UI at `http://localhost:8080` (Default credentials: `airflow`/`airflow`).

### 4. Run Unit Tests

```bash
pytest tests/ -v

```

---

## 🛡️ Data Quality & Testing Strategy

* **Unit Testing (`pytest`):** Validates Pydantic schema validation rules, boundary values, null handling, and date conversions.
* **CI Automation (`GitHub Actions`):** Automated pipeline triggers on all Pull Requests to execute code linters (`ruff`) and integration tests before merging to `main`.
* **Data Observability:** Airflow tasks monitor row counts, schema drift alerts, and null percentage metrics across all pipeline stages.

---

## 📈 Engineering Decisions & Trade-Offs

| Decision | Chosen Approach | Alternative Considered | Rationale |
| --- | --- | --- | --- |
| **Orchestration** | Apache Airflow | Databricks Workflows | Airflow provides enterprise-level multi-platform orchestration across Databricks and Snowflake seamlessly. |
| **Storage Engine** | Delta Lake | Parquet files | Delta Lake provides ACID transactions, Time Travel auditing, and native `MERGE` upsert capabilities. |
| **Data Contracts** | Pydantic | Spark Schema on Read | Pydantic blocks malformed API responses at the ingestion edge before incurring Spark cluster compute costs. |
| **Serving Layer** | Snowflake | Databricks SQL Serverless | Demonstrates multi-cloud interoperability between specialized Big Data compute and cloud data warehousing. |

---

## 👤 Author

### Developed by **Hugo Campos**

### * LinkedIn: [linkedin.com/in/Hugo-Campos](https://www.linkedin.com/in/hugo-campos-b2a678273/)
### * Portfolio / GitHub: [github.com/Hugo-Campos](https://github.com/hugoargerio-glitch?tab=repositories)

`
