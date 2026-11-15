"""Pydantic edge contracts and PySpark schema definitions for MacroFlow Lakehouse."""

from __future__ import annotations

import math
from datetime import date
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

BCB_SCHEMA_FIELDS: tuple[tuple[str, str, bool], ...] = (
    ("reference_date", "date", False),
    ("ticker_or_code", "string", False),
    ("close_price", "double", False),
)

MARKET_SCHEMA_FIELDS: tuple[tuple[str, str, bool], ...] = (
    ("reference_date", "date", False),
    ("ticker", "string", False),
    ("open_price", "double", True),
    ("high_price", "double", True),
    ("low_price", "double", True),
    ("close_price", "double", False),
    ("volume", "double", True),
)


class BCBRecord(BaseModel):
    """Contract for Central Bank of Brazil (BCB SGS) macroeconomic time-series records."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        str_strip_whitespace=True,
    )

    reference_date: date
    ticker_or_code: str = Field(..., min_length=1, description="Nome ou código da série no SGS/BCB")
    close_price: float = Field(..., gt=-100.0, description="Taxa ou índice macroeconômico")

    @field_validator("close_price")
    @classmethod
    def validate_finite_price(cls, value: float) -> float:
        """Ensure close_price is a finite floating-point number."""
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("close_price deve ser um número finito.")
        return numeric

    @model_validator(mode="after")
    def validate_series_domain_bounds(self) -> "BCBRecord":
        """Enforce series-specific economic domain bounds."""
        code = self.ticker_or_code.upper()
        if code == "USD_PTAX" and self.close_price <= 0.0:
            raise ValueError("USD_PTAX deve ser estritamente positivo (> 0).")
        if code == "SELIC" and self.close_price < 0.0:
            raise ValueError("SELIC nominal não pode ser negativa (< 0).")
        return self


class MarketRecord(BaseModel):
    """Contract for Yahoo Finance OHLCV market and yield records."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        str_strip_whitespace=True,
    )

    reference_date: date
    ticker: str = Field(..., min_length=1, description="Identificador do ativo no Yahoo Finance")
    open_price: Optional[float] = Field(default=None, gt=0.0)
    high_price: Optional[float] = Field(default=None, gt=0.0)
    low_price: Optional[float] = Field(default=None, gt=0.0)
    close_price: float = Field(..., gt=0.0, description="Preço/taxa de fechamento estritamente positivo")
    volume: Optional[float] = Field(default=0.0, ge=0.0)

    @model_validator(mode="after")
    def validate_ohlc_consistency(self) -> "MarketRecord":
        """Enforce OHLC price ordering whenever optional bounds are present."""
        if self.high_price is not None and self.low_price is not None and self.high_price < self.low_price:
            raise ValueError("high_price deve ser maior ou igual a low_price.")
        if self.high_price is not None and self.close_price > self.high_price:
            raise ValueError("high_price deve ser maior ou igual a close_price.")
        if self.low_price is not None and self.close_price < self.low_price:
            raise ValueError("low_price deve ser menor ou igual a close_price.")
        if self.high_price is not None and self.open_price is not None and self.open_price > self.high_price:
            raise ValueError("high_price deve ser maior ou igual a open_price.")
        if self.low_price is not None and self.open_price is not None and self.open_price < self.low_price:
            raise ValueError("low_price deve ser menor ou igual a open_price.")
        return self


def get_bcb_spark_schema() -> "StructType":
    """Return the explicit PySpark StructType schema for raw BCB NDJSON records."""
    from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("reference_date", DateType(), nullable=False),
            StructField("ticker_or_code", StringType(), nullable=False),
            StructField("close_price", DoubleType(), nullable=False),
        ]
    )


def get_market_spark_schema() -> "StructType":
    """Return the explicit PySpark StructType schema for raw Yahoo Finance NDJSON records."""
    from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("reference_date", DateType(), nullable=False),
            StructField("ticker", StringType(), nullable=False),
            StructField("open_price", DoubleType(), nullable=True),
            StructField("high_price", DoubleType(), nullable=True),
            StructField("low_price", DoubleType(), nullable=True),
            StructField("close_price", DoubleType(), nullable=False),
            StructField("volume", DoubleType(), nullable=True),
        ]
    )


try:
    BCB_SPARK_SCHEMA: Optional["StructType"] = get_bcb_spark_schema()
    MARKET_SPARK_SCHEMA: Optional["StructType"] = get_market_spark_schema()
except ImportError:
    # PySpark is intentionally absent in lightweight edge ingestion containers.
    BCB_SPARK_SCHEMA = None
    MARKET_SPARK_SCHEMA = None


__all__ = [
    "BCBRecord",
    "MarketRecord",
    "BCB_SCHEMA_FIELDS",
    "MARKET_SCHEMA_FIELDS",
    "BCB_SPARK_SCHEMA",
    "MARKET_SPARK_SCHEMA",
    "get_bcb_spark_schema",
    "get_market_spark_schema",
]
