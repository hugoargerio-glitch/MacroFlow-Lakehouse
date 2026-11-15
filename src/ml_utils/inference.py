"""Batch inference utilities, B3 trading-day calendar stepping, and idempotent Snowflake MERGE helpers."""

from __future__ import annotations

import hashlib
import math
import os
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

import pandas as pd

USDBRL_ASSET_ID = 1

FIXED_B3_HOLIDAYS: frozenset[tuple[int, int]] = frozenset(
    {
        (1, 1),  # Confraternizacao Universal
        (4, 21),  # Tiradentes
        (5, 1),  # Dia do Trabalho
        (9, 7),  # Independencia do Brasil
        (10, 12),  # N. Sra. Aparecida
        (11, 2),  # Finados
        (11, 15),  # Proclamacao da Republica
        (11, 20),  # Consciencia Negra
        (12, 24),  # Vespera de Natal (sem negociacao B3)
        (12, 25),  # Natal
        (12, 31),  # Ultimo dia util do ano (sem expediente B3)
    }
)


def compute_easter_sunday(year: int) -> date:
    """Compute Gregorian Easter Sunday for ``year`` using the Anonymous Gregorian algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def b3_movable_holidays(year: int) -> frozenset[date]:
    """Return B3 movable holidays (Carnaval Mon/Tue, Good Friday, Corpus Christi) for ``year``."""
    easter = compute_easter_sunday(year)
    return frozenset(
        {
            easter - timedelta(days=48),  # Carnaval Segunda
            easter - timedelta(days=47),  # Carnaval Terca
            easter - timedelta(days=2),  # Sexta-Feira Santa
            easter + timedelta(days=60),  # Corpus Christi
        }
    )


def is_b3_trading_day(candidate: date) -> bool:
    """Return True iff ``candidate`` is a valid B3 trading day (mirrors ``DIM_CALENDAR.is_trading_day_b3``)."""
    if candidate.weekday() >= 5:
        return False
    if (candidate.month, candidate.day) in FIXED_B3_HOLIDAYS:
        return False
    if candidate in b3_movable_holidays(candidate.year):
        return False
    return True


def advance_b3_trading_days(reference_date: date, steps: int = 7) -> list[date]:
    """Return the next ``steps`` B3 trading dates strictly after ``reference_date``."""
    if steps <= 0:
        raise ValueError("steps must be positive.")
    trading_dates: list[date] = []
    cursor = reference_date
    while len(trading_dates) < steps:
        cursor += timedelta(days=1)
        if is_b3_trading_day(cursor):
            trading_dates.append(cursor)
    return trading_dates


def validate_feature_freshness(
    reference_date: date,
    as_of_date: date,
    max_staleness_days: int = 5,
) -> None:
    """Ensure the latest Feature Store observation is neither in the future nor stale."""
    if max_staleness_days < 0:
        raise ValueError("max_staleness_days must be >= 0.")
    if reference_date > as_of_date:
        raise ValueError(f"Feature Store reference_date {reference_date} exceeds as_of_date {as_of_date}.")
    age_days = (as_of_date - reference_date).days
    if age_days > max_staleness_days:
        raise ValueError(
            f"Feature Store is stale: latest reference_date={reference_date} is {age_days} days old "
            f"(max allowed={max_staleness_days})."
        )


def build_forecast_id(
    reference_date: date,
    target_horizon_date: date,
    asset_id: int,
    model_version: str,
) -> str:
    """Build deterministic MD5 primary key matching ``FACT_MACRO_FORECASTS.forecast_id``."""
    raw_key = f"{reference_date.isoformat()}_{target_horizon_date.isoformat()}_{asset_id}_{model_version}"
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()


def build_forecast_records(
    reference_date: date,
    current_price: float,
    predictions_df: pd.DataFrame,
    model_version: str,
    asset_id: int = USDBRL_ASSET_ID,
) -> list[dict[str, Any]]:
    """Convert direct log-return predictions (h=1..7) into Snowflake-ready ``FACT_MACRO_FORECASTS`` records."""
    if current_price <= 0.0 or not math.isfinite(current_price):
        raise ValueError(f"current_price must be positive and finite, got {current_price}.")
    if predictions_df.empty:
        raise ValueError("predictions_df cannot be empty.")

    ordered = predictions_df.sort_values("horizon_days").reset_index(drop=True)
    horizons = [int(h) for h in ordered["horizon_days"].tolist()]
    if horizons != list(range(1, len(horizons) + 1)):
        raise ValueError(f"Expected contiguous horizons starting at 1, got {horizons}.")

    target_dates = advance_b3_trading_days(reference_date, steps=len(horizons))
    records: list[dict[str, Any]] = []

    for idx, row in ordered.iterrows():
        horizon = int(row["horizon_days"])
        target_date = target_dates[int(idx)]

        pred_r = float(row["pred_log_return"])
        low_r = min(float(row["lower_log_return"]), pred_r)
        high_r = max(float(row["upper_log_return"]), pred_r)

        pred_price = round(current_price * math.exp(pred_r), 4)
        lower_price = min(round(current_price * math.exp(low_r), 4), pred_price)
        upper_price = max(round(current_price * math.exp(high_r), 4), pred_price)

        forecast_id = build_forecast_id(
            reference_date=reference_date,
            target_horizon_date=target_date,
            asset_id=asset_id,
            model_version=model_version,
        )

        records.append(
            {
                "FORECAST_ID": forecast_id,
                "REFERENCE_DATE": reference_date,
                "TARGET_HORIZON_DATE": target_date,
                "ASSET_ID": int(asset_id),
                "HORIZON_DAYS": int(horizon),
                "PREDICTED_VALUE": Decimal(f"{pred_price:.4f}"),
                "LOWER_BOUND_95": Decimal(f"{lower_price:.4f}"),
                "UPPER_BOUND_95": Decimal(f"{upper_price:.4f}"),
                "MODEL_VERSION": str(model_version)[:50],
            }
        )

    return records


def get_forecast_spark_schema() -> Any:
    """Return explicit PySpark ``StructType`` aligned with ``MACROFLOW_DW.GOLD.FACT_MACRO_FORECASTS``."""
    try:
        from pyspark.sql.types import DateType, DecimalType, IntegerType, ShortType, StringType, StructField, StructType
    except ImportError:
        return None

    return StructType(
        [
            StructField("FORECAST_ID", StringType(), False),
            StructField("REFERENCE_DATE", DateType(), False),
            StructField("TARGET_HORIZON_DATE", DateType(), False),
            StructField("ASSET_ID", IntegerType(), False),
            StructField("HORIZON_DAYS", ShortType(), False),
            StructField("PREDICTED_VALUE", DecimalType(18, 4), False),
            StructField("LOWER_BOUND_95", DecimalType(18, 4), True),
            StructField("UPPER_BOUND_95", DecimalType(18, 4), True),
            StructField("MODEL_VERSION", StringType(), False),
        ]
    )


def _get_secret(scope: str, key: str, fallback_env: str, dbutils_obj: Any = None) -> str:
    if dbutils_obj is not None:
        try:
            val = str(dbutils_obj.secrets.get(scope=scope, key=key)).strip()
            if val:
                return val
        except Exception:  # noqa: BLE001
            pass
    return os.environ.get(fallback_env, "").strip()


def build_snowflake_options(dbutils_obj: Any = None, scope: str = "macroflow_scope") -> dict[str, str]:
    """Build secret-driven Snowflake Spark Connector options with ``MACROFLOW_ETL_ROLE``."""
    account_or_url = _get_secret(scope, "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ACCOUNT", dbutils_obj)
    explicit_url = _get_secret(scope, "SNOWFLAKE_URL", "SNOWFLAKE_URL", dbutils_obj)
    user = _get_secret(scope, "SNOWFLAKE_USER", "SNOWFLAKE_USER", dbutils_obj)
    password = _get_secret(scope, "SNOWFLAKE_PASSWORD", "SNOWFLAKE_PASSWORD", dbutils_obj)

    if explicit_url:
        sf_url = explicit_url
    elif account_or_url:
        sf_url = (
            account_or_url if account_or_url.startswith("http") else f"https://{account_or_url}.snowflakecomputing.com"
        )
    else:
        sf_url = ""

    if not sf_url or "<your_account_identifier>" in sf_url:
        raise ValueError("SNOWFLAKE_ACCOUNT or SNOWFLAKE_URL secret/env var must be configured.")
    if not user or not password:
        raise ValueError("SNOWFLAKE_USER and SNOWFLAKE_PASSWORD secrets/env vars must be configured.")

    return {
        "sfURL": sf_url,
        "sfUser": user,
        "sfPassword": password,
        "sfDatabase": "MACROFLOW_DW",
        "sfSchema": "GOLD",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "MACROFLOW_ETL_ROLE",
    }


def generate_forecast_merge_sql(
    target_table: str = "MACROFLOW_DW.GOLD.FACT_MACRO_FORECASTS",
    staging_table: str = "MACROFLOW_DW.GOLD.STG_FACT_MACRO_FORECASTS",
) -> str:
    """Generate idempotent Snowflake ``MERGE INTO FACT_MACRO_FORECASTS`` SQL on ``forecast_id``."""
    return f"""
    MERGE INTO {target_table} AS target
    USING {staging_table} AS source
    ON target.forecast_id = source.forecast_id
    WHEN MATCHED THEN UPDATE SET
        target.reference_date = source.reference_date,
        target.target_horizon_date = source.target_horizon_date,
        target.asset_id = source.asset_id,
        target.horizon_days = source.horizon_days,
        target.predicted_value = source.predicted_value,
        target.lower_bound_95 = source.lower_bound_95,
        target.upper_bound_95 = source.upper_bound_95,
        target.model_version = source.model_version
    WHEN NOT MATCHED THEN INSERT (
        forecast_id,
        reference_date,
        target_horizon_date,
        asset_id,
        horizon_days,
        predicted_value,
        lower_bound_95,
        upper_bound_95,
        model_version
    ) VALUES (
        source.forecast_id,
        source.reference_date,
        source.target_horizon_date,
        source.asset_id,
        source.horizon_days,
        source.predicted_value,
        source.lower_bound_95,
        source.upper_bound_95,
        source.model_version
    );
    TRUNCATE TABLE IF EXISTS {staging_table};
    """.strip()


FORECAST_SPARK_SCHEMA: Optional[Any] = get_forecast_spark_schema()
