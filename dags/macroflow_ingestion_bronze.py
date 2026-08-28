from datetime import datetime, timedelta
import json
import os
import requests
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator

from contracts.macro_models import BCBRecord, MarketAssetRecord

# Configurações padrão da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

RAW_DATA_PATH = "/opt/airflow/raw_data"


def extract_bcb_series(series_code: int, series_name: str, **kwargs):
    """Extrai séries temporais da API SGS do Banco Central do Brasil com janela móvel."""
    # Define o período recente (últimos 45 dias) para evitar timeout em séries diárias pesadas
    end_date = datetime.now().strftime("%d/%m/%Y")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%d/%m/%Y")

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date,
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    raw_records = response.json()

    # Validação do Contrato de Dados com Pydantic
    validated_data = [BCBRecord(**record).model_dump() for record in raw_records]

    # Particionamento por data de ingestão (year=YYYY/month=MM/day=DD)
    now = datetime.now()
    output_dir = f"{RAW_DATA_PATH}/bronze/bcb/{series_name}/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"
    os.makedirs(output_dir, exist_ok=True)

    file_path = f"{output_dir}/{series_name}_raw.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(validated_data, f, ensure_ascii=False, indent=2)

    print(f"Série {series_name} extraída com sucesso: {len(validated_data)} registros salvos em {file_path}.")

def extract_market_assets(**kwargs):
    """Extrai cotações de mercado via Yahoo Finance."""
    tickers = ["USDBRL=X", "^BVSP", "BTC-BRL"]
    now = datetime.now()
    output_dir = f"{RAW_DATA_PATH}/bronze/market/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"
    os.makedirs(output_dir, exist_ok=True)

    for ticker in tickers:
        asset = yf.Ticker(ticker)
        hist = asset.history(period="1mo")
        hist = hist.reset_index()

        validated_records = []
        for _, row in hist.iterrows():
            record = MarketAssetRecord(
                reference_date=row["Date"].date(),
                ticker=ticker,
                open_price=float(row.get("Open", 0.0)),
                high_price=float(row.get("High", 0.0)),
                low_price=float(row.get("Low", 0.0)),
                close_price=float(row["Close"]),
                volume=float(row.get("Volume", 0.0)),
            )
            validated_records.append(record.model_dump(mode="json"))

        file_path = f"{output_dir}/{ticker.replace('^', '').replace('=', '_')}_raw.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(validated_records, f, ensure_ascii=False, indent=2)

        print(f"Ativo {ticker} extraído com sucesso: {len(validated_records)} registros salvos.")


with DAG(
    dag_id="macroflow_ingestion_bronze",
    default_args=default_args,
    description="Pipeline de ingestão e validação da Camada Bronze do MacroFlow",
    schedule_interval="@daily",
    catchup=False,
    tags=["macroflow", "bronze", "ingestion"],
) as dag:

    task_extract_ipca = PythonOperator(
        task_id="extract_bcb_ipca_433",
        python_callable=extract_bcb_series,
        op_kwargs={"series_code": 433, "series_name": "ipca_mensal"},
    )

    task_extract_selic = PythonOperator(
        task_id="extract_bcb_selic_11",
        python_callable=extract_bcb_series,
        op_kwargs={"series_code": 11, "series_name": "selic_diaria"},
    )

    task_extract_ptax = PythonOperator(
        task_id="extract_bcb_ptax_1",
        python_callable=extract_bcb_series,
        op_kwargs={"series_code": 1, "series_name": "dolar_ptax"},
    )

    task_extract_market = PythonOperator(
        task_id="extract_market_yfinance",
        python_callable=extract_market_assets,
    )

    # Execução das extrações em paralelo na Camada Bronze
    [task_extract_ipca, task_extract_selic, task_extract_ptax, task_extract_market]
