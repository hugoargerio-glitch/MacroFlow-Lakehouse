"""Central Bank of Brazil (BCB SGS) macroeconomic time-series extractor."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from src.extractors.storage import LocalStorageBackend, build_retry_session
from src.schemas.macro_models import BCBRecord

logger = logging.getLogger(__name__)

BCB_SGS_URL_TEMPLATE = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
    "?formato=json&dataInicial={start_date}&dataFinal={end_date}"
)


@dataclass(frozen=True)
class BCBSeriesSpec:
    """Metadata and lookback window specification for a BCB SGS series."""

    name: str
    code: int
    lookback_days: int


BCB_SERIES_SPECS: dict[str, BCBSeriesSpec] = {
    "SELIC": BCBSeriesSpec(name="SELIC", code=11, lookback_days=7),
    "IPCA": BCBSeriesSpec(name="IPCA", code=433, lookback_days=60),
    "USD_PTAX": BCBSeriesSpec(name="USD_PTAX", code=1, lookback_days=7),
}


def coerce_exec_date(logical_date: datetime | date | None) -> date:
    """Validate and normalize an Airflow logical_date or date into a datetime.date."""
    if logical_date is None:
        raise ValueError("logical_date is required for deterministic partitioned extraction.")
    if isinstance(logical_date, datetime):
        return logical_date.date()
    return logical_date


def resolve_lookback_days(series_code: int, series_name: str, lookback_days: Optional[int] = None) -> int:
    """Resolve sliding lookback window days (IPCA monthly series defaults to 60 days)."""
    if lookback_days is not None:
        if lookback_days < 0:
            raise ValueError("lookback_days must be >= 0.")
        return lookback_days
    spec = BCB_SERIES_SPECS.get(series_name.upper())
    if spec is not None:
        return spec.lookback_days
    return 60 if series_code == 433 else 7


def extract_bcb_series(
    series_code: int,
    series_name: str,
    logical_date: datetime | date | None = None,
    raw_base_path: str | Path = "/opt/airflow/raw_data",
    lookback_days: Optional[int] = None,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> Optional[Path]:
    """Fetch, validate, and write a BCB SGS series as Hive-partitioned NDJSON."""
    exec_date = coerce_exec_date(logical_date)
    window_days = resolve_lookback_days(series_code=series_code, series_name=series_name, lookback_days=lookback_days)

    start_date = (exec_date - timedelta(days=window_days)).strftime("%d/%m/%Y")
    end_date = exec_date.strftime("%d/%m/%Y")
    url = BCB_SGS_URL_TEMPLATE.format(
        series_code=series_code,
        start_date=start_date,
        end_date=end_date,
    )

    http = session or build_retry_session()
    response = http.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Unexpected BCB payload type for {series_name}: {type(payload).__name__}")

    validated_records: list[BCBRecord] = []
    for item in payload:
        day, month, year = str(item["data"]).split("/")
        raw_value = str(item["valor"]).strip().replace(",", ".")
        record = BCBRecord(
            reference_date=date(int(year), int(month), int(day)),
            ticker_or_code=series_name,
            close_price=float(raw_value),
        )
        validated_records.append(record)

    if not validated_records:
        logger.warning(
            "BCB series %s (SGS %s) returned 0 records for window %s..%s.",
            series_name,
            series_code,
            start_date,
            end_date,
        )
        return None

    storage = LocalStorageBackend(raw_base_path)
    return storage.write_records(
        source="bcb",
        exec_date=exec_date,
        filename=f"{series_name}.json",
        records=validated_records,
    )
