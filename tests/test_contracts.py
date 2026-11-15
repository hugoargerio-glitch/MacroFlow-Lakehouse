"""Unit tests for MacroFlow Pydantic contracts and Spark schema definitions."""

from __future__ import annotations

import math
from datetime import date

import pytest
from pydantic import ValidationError

from src.schemas.macro_models import (
    BCB_SCHEMA_FIELDS,
    MARKET_SCHEMA_FIELDS,
    BCBRecord,
    MarketRecord,
    get_bcb_spark_schema,
    get_market_spark_schema,
)


def test_bcb_record_valid_and_json_dump() -> None:
    """Validate valid BCB records including negative IPCA deflation and JSON serialization."""
    selic = BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=0.0551)
    ipca_deflation = BCBRecord(reference_date="2026-03-10", ticker_or_code="IPCA", close_price=-0.68)
    ptax = BCBRecord(reference_date="2026-03-10", ticker_or_code="USD_PTAX", close_price=5.125)

    assert selic.reference_date == date(2026, 3, 10)
    assert selic.model_dump(mode="json") == {
        "reference_date": "2026-03-10",
        "ticker_or_code": "SELIC",
        "close_price": 0.0551,
    }
    assert ipca_deflation.close_price == -0.68
    assert ptax.close_price == 5.125


def test_bcb_record_rejects_domain_and_nan_violations() -> None:
    """Reject null, non-finite, out-of-bounds, empty-code, extra-field, and bad-date inputs."""
    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=None)  # type: ignore[arg-type]

    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="IPCA", close_price=-150.0)

    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=-0.01)

    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="USD_PTAX", close_price=0.0)

    for bad_float in (math.nan, math.inf, -math.inf):
        with pytest.raises(ValidationError):
            BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=bad_float)

    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="   ", close_price=1.0)

    with pytest.raises(ValidationError):
        BCBRecord(reference_date="2026-03-10", ticker_or_code="SELIC", close_price=1.0, unexpected="x")  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        BCBRecord(reference_date="10/03/2026", ticker_or_code="USD_PTAX", close_price=5.125)  # type: ignore[arg-type]


def test_market_record_valid_with_optional_ohlc() -> None:
    """Validate regular OHLCV quotes and yield proxies with omitted intraday OHLC bounds."""
    fx = MarketRecord(
        reference_date="2026-03-10",
        ticker="USDBRL=X",
        open_price=5.10,
        high_price=5.15,
        low_price=5.09,
        close_price=5.142,
        volume=1500000.0,
    )
    irx = MarketRecord(reference_date="2026-03-10", ticker="^IRX", close_price=4.25, volume=0.0)

    assert fx.ticker == "USDBRL=X"
    assert irx.open_price is None
    assert irx.volume == 0.0


def test_market_record_rejects_invalid_prices_ohlc_and_nan() -> None:
    """Reject <=0 close, NaN/Inf, negative volume, inverted OHLC, empty ticker, and extra keys."""
    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="^BVSP", close_price=0.0)

    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="BTC-BRL", close_price=-100.5)

    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", close_price=5.20)  # type: ignore[call-arg]

    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="   ", close_price=5.20)

    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="USDBRL=X", close_price=5.20, volume=-1.0)

    for bad_float in (math.nan, math.inf, -math.inf):
        with pytest.raises(ValidationError):
            MarketRecord(reference_date="2026-03-10", ticker="USDBRL=X", open_price=bad_float, close_price=5.20)
        with pytest.raises(ValidationError):
            MarketRecord(reference_date="2026-03-10", ticker="USDBRL=X", close_price=bad_float)

    # Inverted high < low
    with pytest.raises(ValidationError):
        MarketRecord(
            reference_date="2026-03-10",
            ticker="USDBRL=X",
            open_price=5.10,
            high_price=5.05,
            low_price=5.08,
            close_price=5.07,
        )

    # close_price > high_price
    with pytest.raises(ValidationError):
        MarketRecord(
            reference_date="2026-03-10",
            ticker="USDBRL=X",
            high_price=5.10,
            low_price=5.00,
            close_price=5.12,
        )

    # Extra field forbidden
    with pytest.raises(ValidationError):
        MarketRecord(reference_date="2026-03-10", ticker="USDBRL=X", close_price=5.20, extra_col=123)  # type: ignore[call-arg]


def test_schema_field_contracts_and_spark_schema_helper() -> None:
    """Verify canonical schema metadata and Spark StructType helper behavior."""
    assert [name for name, _, _ in BCB_SCHEMA_FIELDS] == ["reference_date", "ticker_or_code", "close_price"]
    assert [name for name, _, _ in MARKET_SCHEMA_FIELDS] == [
        "reference_date",
        "ticker",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]

    try:
        import pyspark  # noqa: F401
    except ImportError:
        with pytest.raises(ImportError):
            get_bcb_spark_schema()
        with pytest.raises(ImportError):
            get_market_spark_schema()
    else:
        bcb_schema = get_bcb_spark_schema()
        market_schema = get_market_spark_schema()
        assert [f.name for f in bcb_schema.fields] == ["reference_date", "ticker_or_code", "close_price"]
        assert [f.name for f in market_schema.fields] == [
            "reference_date",
            "ticker",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
        ]
