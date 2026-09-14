import json
import os
from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator

from airflow import DAG

RAW_BASE_PATH = "/opt/airflow/raw_data"

SERIES_BCB = {
    "SELIC": 11,  # Selic Diária
    "IPCA": 433,  # IPCA Mensal
    "USD_PTAX": 1,  # Câmbio PTAX Livre
}

TICKERS_YFINANCE = ["USDBRL=X", "^BVSP", "BTC-BRL"]

default_args = {
    "owner": "macroflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def extract_bcb_series(series_code: int, series_name: str, logical_date=None, **kwargs):
    """Extrai séries temporais do Banco Central do Brasil utilizando o contexto de data da DAG."""
    import requests

    from src.schemas.macro_models import BCBRecord

    exec_date = logical_date.date()
    # Janela deslizante de 5 dias retroativos a partir da data de execução lógica
    start_date = (exec_date - timedelta(days=5)).strftime("%d/%m/%Y")
    end_date = exec_date.strftime("%d/%m/%Y")

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados?formato=json&dataInicial={start_date}&dataFinal={end_date}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    validated_data = []
    for item in payload:
        day, month, year = item["data"].split("/")
        record = BCBRecord(
            reference_date=f"{year}-{month}-{day}",
            ticker_or_code=series_name,
            close_price=float(str(item["valor"]).replace(",", ".")),
        )
        validated_data.append(record.model_dump(mode="json"))

    year_part = exec_date.strftime("%Y")
    month_part = exec_date.strftime("%m")
    day_part = exec_date.strftime("%d")
    output_dir = os.path.join(RAW_BASE_PATH, "bcb", f"year={year_part}", f"month={month_part}", f"day={day_part}")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f"{series_name}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(validated_data, f, ensure_ascii=False, indent=2)


def extract_yfinance_assets(tickers: list, logical_date=None, **kwargs):
    """Extrai cotações de mercado via Yahoo Finance respeitando a data lógica da execução."""
    import yfinance as yf

    from src.schemas.macro_models import MarketRecord

    exec_date = logical_date.date()
    # Intervalo delimitado para garantir idempotência em reexecuções retroativas
    start_date = (exec_date - timedelta(days=5)).strftime("%Y-%m-%d")
    end_date = (exec_date + timedelta(days=1)).strftime("%Y-%m-%d")

    validated_data = []
    for ticker in tickers:
        ticker_obj = yf.Ticker(ticker)
        df_hist = ticker_obj.history(start=start_date, end=end_date, interval="1d")

        if df_hist.empty:
            continue

        df_hist = df_hist.reset_index()
        for _, row in df_hist.iterrows():
            ref_date = row["Date"].date().strftime("%Y-%m-%d")
            record = MarketRecord(
                reference_date=ref_date,
                ticker=ticker,
                open_price=float(row.get("Open", 0.0)),
                high_price=float(row.get("High", 0.0)),
                low_price=float(row.get("Low", 0.0)),
                close_price=float(row["Close"]),
                volume=float(row.get("Volume", 0.0)),
            )
            validated_data.append(record.model_dump(mode="json"))

    year_part = exec_date.strftime("%Y")
    month_part = exec_date.strftime("%m")
    day_part = exec_date.strftime("%d")
    output_dir = os.path.join(RAW_BASE_PATH, "yfinance", f"year={year_part}", f"month={month_part}", f"day={day_part}")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, "market_quotes.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(validated_data, f, ensure_ascii=False, indent=2)


with DAG(
    dag_id="macroflow_ingestion_bronze",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval="0 21 * * 1-5",  # Dias úteis às 21:00 UTC
    catchup=False,
    tags=["bronze", "ingestion", "shift-left", "bcb", "yfinance"],
) as dag:
    task_yfinance = PythonOperator(
        task_id="extract_yfinance_market_data",
        python_callable=extract_yfinance_assets,
        op_kwargs={"tickers": TICKERS_YFINANCE},
    )

    for series_name, series_code in SERIES_BCB.items():
        task_bcb = PythonOperator(
            task_id=f"extract_bcb_{series_name.lower()}",
            python_callable=extract_bcb_series,
            op_kwargs={"series_code": series_code, "series_name": series_name},
        )
        task_bcb >> task_yfinance
