"""Unit tests for modular BCB/Yahoo extractors, NDJSON/Databricks storage, and Airflow DAGs."""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.extractors.airflow_helpers import build_databricks_submit_kwargs, notify_task_failure
from src.extractors.bcb import coerce_exec_date, extract_bcb_series, resolve_lookback_days
from src.extractors.storage import (
    DatabricksFilesUploader,
    build_retry_session,
    upload_raw_partition,
    write_ndjson,
)
from src.extractors.yfinance_extractor import DEFAULT_YFINANCE_TICKERS, extract_yfinance_assets


def test_coerce_exec_date_and_lookback_resolution() -> None:
    """Verify logical_date normalization and BCB lookback defaults (60d for IPCA)."""
    assert coerce_exec_date(datetime(2026, 3, 10, 21, 0, tzinfo=timezone.utc)) == date(2026, 3, 10)
    assert coerce_exec_date(date(2026, 3, 10)) == date(2026, 3, 10)
    with pytest.raises(ValueError, match="logical_date"):
        coerce_exec_date(None)

    assert resolve_lookback_days(11, "SELIC") == 7
    assert resolve_lookback_days(433, "IPCA") == 60
    assert resolve_lookback_days(1, "USD_PTAX") == 7
    assert resolve_lookback_days(11, "SELIC", lookback_days=14) == 14
    with pytest.raises(ValueError):
        resolve_lookback_days(11, "SELIC", lookback_days=-1)


def test_extract_bcb_series_writes_ndjson_with_60d_ipca_lookback(tmp_path: Path) -> None:
    """Validate BCB SGS comma-decimal parsing, NDJSON output, and IPCA 60-day query window."""
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"data": "10/02/2026", "valor": "0,52"},
        {"data": "10/03/2026", "valor": "-0,15"},
    ]
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    out_path = extract_bcb_series(
        series_code=433,
        series_name="IPCA",
        logical_date=datetime(2026, 3, 10, 21, 0, tzinfo=timezone.utc),
        raw_base_path=tmp_path,
        session=mock_session,
    )

    assert out_path == tmp_path / "bcb" / "year=2026" / "month=03" / "day=10" / "IPCA.json"
    assert out_path.exists()

    called_url = mock_session.get.call_args.args[0]
    assert "dataInicial=09/01/2026" in called_url
    assert "dataFinal=10/03/2026" in called_url

    lines = out_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0] == {"reference_date": "2026-02-10", "ticker_or_code": "IPCA", "close_price": 0.52}
    assert parsed[1] == {"reference_date": "2026-03-10", "ticker_or_code": "IPCA", "close_price": -0.15}


def test_extract_bcb_series_skips_empty_payload_and_rejects_bad_type(tmp_path: Path) -> None:
    """Empty BCB payloads must not create corrupt empty '[]' files."""
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    out_path = extract_bcb_series(
        series_code=11,
        series_name="SELIC",
        logical_date=date(2026, 3, 10),
        raw_base_path=tmp_path,
        session=mock_session,
    )
    assert out_path is None
    assert not (tmp_path / "bcb" / "year=2026" / "month=03" / "day=10" / "SELIC.json").exists()

    mock_response.json.return_value = {"error": "unexpected"}
    with pytest.raises(ValueError, match="Unexpected BCB payload"):
        extract_bcb_series(
            series_code=11,
            series_name="SELIC",
            logical_date=date(2026, 3, 10),
            raw_base_path=tmp_path,
            session=mock_session,
        )


def test_build_retry_session_mounts_https_retry_adapter() -> None:
    """Verify HTTP retry adapter configuration for 429/5xx resilience."""
    session = build_retry_session(total_retries=4, backoff_factor=0.5)
    adapter = session.get_adapter("https://api.bcb.gov.br")
    assert adapter.max_retries.total == 4
    assert 429 in adapter.max_retries.status_forcelist
    assert 503 in adapter.max_retries.status_forcelist


def test_extract_yfinance_assets_isolates_errors_and_sanitizes_nan(tmp_path: Path) -> None:
    """One failing ticker must not abort valid tickers; NaN OHLCV must never emit invalid JSON NaN."""
    assert "^IRX" in DEFAULT_YFINANCE_TICKERS

    def fake_factory(ticker: str) -> Any:
        client = MagicMock()
        if ticker == "BROKEN":
            client.history.side_effect = RuntimeError("HTTP 503 upstream")
            return client
        if ticker == "EMPTY":
            client.history.return_value = pd.DataFrame()
            return client

        df = pd.DataFrame(
            {
                "Date": [pd.Timestamp("2026-03-10"), pd.Timestamp("2026-03-11")],
                "Open": [math.nan, 5.10],
                "High": [5.20, 5.25],
                "Low": [5.05, 5.08],
                "Close": [5.15, math.nan],
                "Volume": [math.nan, 1000.0],
            }
        )
        client.history.return_value = df
        return client

    result = extract_yfinance_assets(
        tickers=["USDBRL=X", "BROKEN", "EMPTY", "^IRX"],
        logical_date=date(2026, 3, 10),
        raw_base_path=tmp_path,
        ticker_client_factory=fake_factory,
        fail_on_partial=False,
    )

    assert result.output_path == tmp_path / "yfinance" / "year=2026" / "month=03" / "day=10" / "market_quotes.json"
    assert result.records_count == 2
    assert result.succeeded_tickers == ["USDBRL=X", "EMPTY", "^IRX"]
    assert "BROKEN" in result.failed_tickers

    raw_text = result.output_path.read_text(encoding="utf-8")
    assert "NaN" not in raw_text
    rows = [json.loads(line) for line in raw_text.strip().splitlines()]
    assert rows[0]["ticker"] == "USDBRL=X"
    assert rows[0]["open_price"] is None
    assert rows[0]["volume"] == 0.0
    assert rows[1]["ticker"] == "^IRX"


def test_extract_yfinance_assets_raises_when_all_fail_or_fail_on_partial(tmp_path: Path) -> None:
    """Raise when all tickers fail, and raise after persisting when fail_on_partial=True."""

    def always_fail(ticker: str) -> Any:
        client = MagicMock()
        client.history.side_effect = RuntimeError("down")
        return client

    with pytest.raises(RuntimeError, match="All Yahoo Finance ticker extractions failed"):
        extract_yfinance_assets(
            tickers=["USDBRL=X"],
            logical_date=date(2026, 3, 10),
            raw_base_path=tmp_path,
            ticker_client_factory=always_fail,
        )

    def one_good_one_bad(ticker: str) -> Any:
        client = MagicMock()
        if ticker == "BAD":
            client.history.side_effect = RuntimeError("bad")
        else:
            client.history.return_value = pd.DataFrame(
                {"Date": [pd.Timestamp("2026-03-10")], "Open": [5.0], "High": [5.1], "Low": [4.9], "Close": [5.05]}
            )
        return client

    with pytest.raises(RuntimeError, match="Partial Yahoo Finance extraction failure"):
        extract_yfinance_assets(
            tickers=["USDBRL=X", "BAD"],
            logical_date=date(2026, 3, 10),
            raw_base_path=tmp_path,
            ticker_client_factory=one_good_one_bad,
            fail_on_partial=True,
        )
    # Valid data was still written before raising
    assert (tmp_path / "yfinance" / "year=2026" / "month=03" / "day=10" / "market_quotes.json").exists()


def test_write_ndjson_forbids_nan_and_uploader_syncs_volumes_and_dbfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify strict NDJSON allow_nan=False and Databricks Files/DBFS upload requests."""
    with pytest.raises(ValueError):
        write_ndjson([{"bad": math.nan}], tmp_path / "bad.json")

    bcb_file = tmp_path / "bcb" / "year=2026" / "month=03" / "day=10" / "SELIC.json"
    yf_file = tmp_path / "yfinance" / "year=2026" / "month=03" / "day=10" / "market_quotes.json"
    write_ndjson([{"reference_date": "2026-03-10", "ticker_or_code": "SELIC", "close_price": 0.05}], bcb_file)
    write_ndjson([{"reference_date": "2026-03-10", "ticker": "USDBRL=X", "close_price": 5.1}], yf_file)

    mock_session = MagicMock()
    mock_session.put.return_value = MagicMock(raise_for_status=MagicMock())
    mock_session.post.return_value = MagicMock(raise_for_status=MagicMock())

    uploader = DatabricksFilesUploader(host="https://dbc.example.com/", token="tok", session=mock_session)
    uploaded_uc = uploader.sync_partition_date(
        local_base_path=tmp_path,
        exec_date=date(2026, 3, 10),
        remote_base_path="/Volumes/macroflow/bronze/raw_data",
    )
    assert uploaded_uc == [
        "/Volumes/macroflow/bronze/raw_data/bcb/year=2026/month=03/day=10/SELIC.json",
        "/Volumes/macroflow/bronze/raw_data/yfinance/year=2026/month=03/day=10/market_quotes.json",
    ]
    assert mock_session.put.call_count == 2

    uploaded_dbfs = uploader.upload_file(bcb_file, "dbfs:/mnt/raw_data/bcb/year=2026/month=03/day=10/SELIC.json")
    assert uploaded_dbfs == "/mnt/raw_data/bcb/year=2026/month=03/day=10/SELIC.json"
    assert mock_session.post.call_count == 2

    monkeypatch.setenv("DATABRICKS_HOST", "https://dbc.example.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
    monkeypatch.setattr("src.extractors.storage.build_retry_session", lambda: mock_session)
    assert len(upload_raw_partition(date(2026, 3, 10), local_base_path=tmp_path)) == 2


def test_airflow_helpers_and_dag_integrity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validate DagBag loading, parallel edge tasks, Lakehouse chain, and ML trigger wiring."""
    airflow = pytest.importorskip("airflow")
    assert airflow is not None

    import inspect

    from airflow.models import DagBag

    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    dags_dir = Path(__file__).resolve().parent.parent / "airflow" / "dags"
    dagbag_kwargs: dict[str, Any] = {"dag_folder": str(dags_dir)}
    if "include_examples" in inspect.signature(DagBag.__init__).parameters:
        dagbag_kwargs["include_examples"] = False
    dag_bag = DagBag(**dagbag_kwargs)
    assert dag_bag.import_errors == {}
    assert {"macroflow_ingestion_bronze", "macroflow_ml_inference_batch"}.issubset(dag_bag.dag_ids)

    bronze_dag = dag_bag.dags["macroflow_ingestion_bronze"]
    ml_dag = dag_bag.dags["macroflow_ml_inference_batch"]

    assert bronze_dag.max_active_runs == 1
    assert ml_dag.max_active_runs == 1
    assert getattr(bronze_dag, "schedule", getattr(bronze_dag, "schedule_interval", None)) == "0 21 * * 1-5"
    assert getattr(ml_dag, "schedule", getattr(ml_dag, "schedule_interval", "UNSET")) is None

    # Parallel extraction: yfinance must NOT depend on any BCB task
    yf_task = bronze_dag.get_task("extract_yfinance_market_data")
    assert yf_task.upstream_task_ids == set()

    upload_task = bronze_dag.get_task("upload_raw_to_databricks")
    assert upload_task.upstream_task_ids == {
        "extract_bcb_selic",
        "extract_bcb_ipca",
        "extract_bcb_usd_ptax",
        "extract_yfinance_market_data",
    }

    trigger_task = bronze_dag.get_task("trigger_ml_inference_batch")
    assert trigger_task.trigger_dag_id == "macroflow_ml_inference_batch"
    assert trigger_task.upstream_task_ids == {
        "databricks_lakehouse_pipeline.run_train_and_register",
        "databricks_lakehouse_pipeline.run_snowflake_gold_sync",
    }

    scoring_task = ml_dag.get_task("execute_lgbm_batch_scoring")
    assert scoring_task.libraries
    assert "dbfs:/FileStore" not in scoring_task.spark_python_task["python_file"]
    assert scoring_task.new_cluster["spark_env_vars"]["MLFLOW_TRACKING_URI"] == "databricks"

    kwargs = build_databricks_submit_kwargs("ml/03_batch_predict.py", is_ml=True)
    assert kwargs["databricks_conn_id"] == "databricks_default"

    mock_logger = MagicMock()
    monkeypatch.setattr("src.extractors.airflow_helpers.logger", mock_logger)
    notify_task_failure({"dag": "macroflow_ingestion_bronze", "logical_date": "2026-03-10"})
    mock_logger.error.assert_called_once()
    assert "AIRFLOW_TASK_FAILURE" in mock_logger.error.call_args.args[0]
