"""Yahoo Finance market & yield quotes extractor with per-ticker isolation."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import pandas as pd

from src.extractors.bcb import coerce_exec_date
from src.extractors.storage import LocalStorageBackend
from src.schemas.macro_models import MarketRecord

logger = logging.getLogger(__name__)

DEFAULT_YFINANCE_TICKERS: tuple[str, ...] = (
    "USDBRL=X",
    "^BVSP",
    "BTC-BRL",
    "^IRX",  # 13-Week US Treasury Bill yield proxy
)


@dataclass
class YFinanceExtractionResult:
    """Structured extraction summary preserving per-ticker isolation status."""

    output_path: Optional[Path]
    records_count: int
    succeeded_tickers: list[str] = field(default_factory=list)
    failed_tickers: dict[str, str] = field(default_factory=dict)


def _sanitize_optional_positive(value: Any) -> Optional[float]:
    """Return a finite positive float or None when Yahoo Finance emits NaN/missing/<=0."""
    if value is None or pd.isna(value):
        return None
    numeric = float(value)
    if not math.isfinite(numeric) or numeric <= 0.0:
        return None
    return numeric


def _sanitize_volume(value: Any) -> float:
    """Return a finite non-negative volume float, defaulting missing/NaN to 0.0."""
    if value is None or pd.isna(value):
        return 0.0
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0:
        return 0.0
    return numeric


def _extract_row_date(raw_date: Any) -> date:
    """Normalize a DataFrame Date column value to datetime.date."""
    if isinstance(raw_date, datetime):
        return raw_date.date()
    if isinstance(raw_date, date):
        return raw_date
    if hasattr(raw_date, "date"):
        return raw_date.date()
    return date.fromisoformat(str(raw_date)[:10])


def _default_ticker_factory(ticker: str) -> Any:
    import yfinance as yf

    return yf.Ticker(ticker)


def extract_yfinance_assets(
    tickers: Sequence[str] = DEFAULT_YFINANCE_TICKERS,
    logical_date: datetime | date | None = None,
    raw_base_path: str | Path = "/opt/airflow/raw_data",
    lookback_days: int = 7,
    timeout: int = 30,
    ticker_client_factory: Optional[Callable[[str], Any]] = None,
    fail_on_partial: bool = False,
) -> YFinanceExtractionResult:
    """Fetch, contract-validate, and persist Yahoo Finance OHLCV rows as NDJSON."""
    exec_date = coerce_exec_date(logical_date)
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0.")
    if not tickers:
        raise ValueError("tickers sequence must not be empty.")

    start_date = (exec_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = (exec_date + timedelta(days=1)).strftime("%Y-%m-%d")
    factory = ticker_client_factory or _default_ticker_factory

    validated_records: list[MarketRecord] = []
    succeeded: list[str] = []
    failed: dict[str, str] = {}

    for ticker in tickers:
        try:
            ticker_obj = factory(ticker)
            try:
                df_hist = ticker_obj.history(start=start_date, end=end_date, interval="1d", timeout=timeout)
            except TypeError:
                df_hist = ticker_obj.history(start=start_date, end=end_date, interval="1d")

            if df_hist is None or df_hist.empty:
                logger.warning("Yahoo Finance returned empty DataFrame for %s (%s..%s).", ticker, start_date, end_date)
                succeeded.append(ticker)
                continue

            df_reset = df_hist.reset_index()
            ticker_records: list[MarketRecord] = []
            for _, row in df_reset.iterrows():
                raw_close = row.get("Close")
                if raw_close is None or pd.isna(raw_close):
                    continue
                close_val = float(raw_close)
                record = MarketRecord(
                    reference_date=_extract_row_date(row["Date"]),
                    ticker=ticker,
                    open_price=_sanitize_optional_positive(row.get("Open")),
                    high_price=_sanitize_optional_positive(row.get("High")),
                    low_price=_sanitize_optional_positive(row.get("Low")),
                    close_price=close_val,
                    volume=_sanitize_volume(row.get("Volume")),
                )
                ticker_records.append(record)

            validated_records.extend(ticker_records)
            succeeded.append(ticker)
        except Exception as exc:
            logger.exception("Failed extracting Yahoo Finance ticker %s: %s", ticker, exc)
            failed[ticker] = str(exc)

    if not succeeded and failed:
        raise RuntimeError(f"All Yahoo Finance ticker extractions failed: {failed}")

    storage = LocalStorageBackend(raw_base_path)
    output_path = storage.write_records(
        source="yfinance",
        exec_date=exec_date,
        filename="market_quotes.json",
        records=validated_records,
    )

    result = YFinanceExtractionResult(
        output_path=output_path,
        records_count=len(validated_records),
        succeeded_tickers=succeeded,
        failed_tickers=failed,
    )

    if fail_on_partial and failed:
        raise RuntimeError(f"Partial Yahoo Finance extraction failure (valid data persisted): {failed}")

    return result
