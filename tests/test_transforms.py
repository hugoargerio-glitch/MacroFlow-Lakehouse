"""Unit tests importing and verifying real production ETL transforms in ``src/transforms``."""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(scope="session")
def spark() -> Any:
    """Create an isolated local PySpark session, skipping cleanly when Spark/Delta are absent."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("MacroFlow-Transform-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_bronze_reads_ndjson_with_hive_partitions_and_handles_missing_partition(
    spark: Any,
    tmp_path: Path,
) -> None:
    """Verify strict NDJSON reading, Hive partition retention, and missing-path safety."""
    from src.schemas.macro_models import get_bcb_spark_schema, get_market_spark_schema
    from src.transforms.bronze import read_raw_ndjson_batch

    bcb_dir = tmp_path / "bcb" / "year=2026" / "month=11" / "day=15"
    bcb_dir.mkdir(parents=True)
    bcb_file = bcb_dir / "SELIC.json"
    bcb_rows = [
        {"reference_date": "2026-11-13", "ticker_or_code": "SELIC", "close_price": 0.050788},
        {"reference_date": "2026-11-14", "ticker_or_code": "SELIC", "close_price": 0.050788},
    ]
    bcb_file.write_text("\n".join(json.dumps(r) for r in bcb_rows) + "\n", encoding="utf-8")

    yf_dir = tmp_path / "yfinance" / "year=2026" / "month=11" / "day=15"
    yf_dir.mkdir(parents=True)
    yf_file = yf_dir / "market_quotes.json"
    yf_rows = [
        {
            "reference_date": "2026-11-13",
            "ticker": "USDBRL=X",
            "open_price": 5.75,
            "high_price": 5.80,
            "low_price": 5.70,
            "close_price": 5.78,
            "volume": 12000.0,
        }
    ]
    yf_file.write_text("\n".join(json.dumps(r) for r in yf_rows) + "\n", encoding="utf-8")

    fixed_ts = datetime(2026, 11, 15, 21, 0, 0, tzinfo=timezone.utc)
    df_bcb = read_raw_ndjson_batch(
        spark,
        source="bcb",
        schema=get_bcb_spark_schema(),
        raw_base_path=str(tmp_path),
        target_date=date(2026, 11, 15),
        ingestion_ts=fixed_ts,
    )
    bcb_collected = df_bcb.orderBy("reference_date").collect()
    assert len(bcb_collected) == 2
    assert bcb_collected[0]["year"] == 2026
    assert bcb_collected[0]["month"] == 11
    assert bcb_collected[0]["day"] == 15
    assert bcb_collected[0]["source_partition_date"] == date(2026, 11, 15)
    assert bcb_collected[0]["source_file"].endswith("SELIC.json")

    df_yf = read_raw_ndjson_batch(
        spark,
        source="yfinance",
        schema=get_market_spark_schema(),
        raw_base_path=str(tmp_path),
        target_date=date(2026, 11, 15),
        ingestion_ts=fixed_ts,
    )
    assert df_yf.count() == 1

    df_missing = read_raw_ndjson_batch(
        spark,
        source="bcb",
        schema=get_bcb_spark_schema(),
        raw_base_path=str(tmp_path),
        target_date=date(2026, 11, 16),
    )
    assert df_missing.count() == 0
    assert "source_partition_date" in df_missing.columns


def test_bronze_deduplicate_against_existing_files(spark: Any) -> None:
    """Verify already-ingested source_file paths are excluded on re-runs."""
    from src.transforms.bronze import deduplicate_against_existing_files

    df_incoming = spark.createDataFrame(
        [
            (date(2026, 11, 13), "SELIC", 0.05, "file:///raw/day=14/SELIC.json"),
            (date(2026, 11, 14), "SELIC", 0.05, "file:///raw/day=15/SELIC.json"),
        ],
        ["reference_date", "ticker_or_code", "close_price", "source_file"],
    )
    df_existing = spark.createDataFrame(
        [("file:///raw/day=14/SELIC.json",)],
        ["source_file"],
    )
    df_new = deduplicate_against_existing_files(df_incoming, df_existing).collect()
    assert len(df_new) == 1
    assert df_new[0]["source_file"] == "file:///raw/day=15/SELIC.json"


def test_silver_deterministic_dedup_precision_and_no_synthetic_ffill(spark: Any) -> None:
    """Verify Silver selects latest ingestion_timestamp, keeps Decimal(18,8), and skips fake ffill."""
    from src.transforms.silver import build_silver_conformed, filter_incremental_bronze

    ts_old = datetime(2026, 11, 14, 21, 0, 0, tzinfo=timezone.utc)
    ts_new = datetime(2026, 11, 15, 21, 0, 0, tzinfo=timezone.utc)

    df_bcb_bronze = spark.createDataFrame(
        [
            (date(2026, 11, 13), "SELIC", 0.045073, date(2026, 11, 14), "f1.json", ts_old),
            (date(2026, 11, 13), "USD_PTAX", -1.0, date(2026, 11, 14), "bad.json", ts_old),
        ],
        [
            "reference_date",
            "ticker_or_code",
            "close_price",
            "source_partition_date",
            "source_file",
            "ingestion_timestamp",
        ],
    )
    df_yf_bronze = spark.createDataFrame(
        [
            # Older preliminary quote was higher (5.85); newer revised quote is lower (5.72) -> must pick 5.72
            (date(2026, 11, 13), "USDBRL=X", 5.80, 5.86, 5.75, 5.85, 100.0, date(2026, 11, 14), "f1.json", ts_old),
            (date(2026, 11, 13), "USDBRL=X", 5.70, 5.78, 5.68, 5.72, 120.0, date(2026, 11, 15), "f2.json", ts_new),
            # Monday observation after weekend gap -> weekend dates must NOT be synthesized
            (date(2026, 11, 16), "USDBRL=X", 5.72, 5.76, 5.70, 5.74, 150.0, date(2026, 11, 16), "f3.json", ts_new),
        ],
        [
            "reference_date",
            "ticker",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "source_partition_date",
            "source_file",
            "ingestion_timestamp",
        ],
    )

    df_silver = build_silver_conformed(df_bcb_bronze, df_yf_bronze, silver_ingestion_ts=ts_new)
    rows = df_silver.orderBy("ticker", "reference_date").collect()

    assert len(rows) == 3  # SELIC (11-13), USDBRL=X (11-13), USDBRL=X (11-16); invalid USD_PTAX dropped
    selic_row = next(r for r in rows if r["ticker"] == "SELIC")
    assert selic_row["close_price"] == Decimal("0.04507300")

    usd_rows = [r for r in rows if r["ticker"] == "USDBRL=X"]
    assert [r["reference_date"] for r in usd_rows] == [date(2026, 11, 13), date(2026, 11, 16)]
    assert usd_rows[0]["close_price"] == Decimal("5.72000000")

    df_inc = filter_incremental_bronze(df_yf_bronze, watermark_ts=ts_old)
    assert df_inc.count() == 2


def test_gold_schema_and_macro_rate_separation(spark: Any) -> None:
    """Verify exact 14-column Snowflake contract and NULL return/SMA/vol for macro rate series."""
    from pyspark.sql.types import DateType, DecimalType, StringType, TimestampType

    from src.transforms.gold import build_gold_fact_market_daily

    silver_rows = [
        (date(2026, 1, 2), "SELIC", Decimal("0.05078800")),
        (date(2026, 1, 5), "SELIC", Decimal("0.05078800")),
        (date(2026, 1, 2), "USDBRL=X", Decimal("5.00000000")),
        (date(2026, 1, 5), "USDBRL=X", Decimal("5.50000000")),
    ]
    df_silver = spark.createDataFrame(silver_rows, ["reference_date", "ticker", "close_price"])
    fixed_calc_ts = datetime(2026, 1, 5, 22, 0, 0, tzinfo=timezone.utc)

    df_gold = build_gold_fact_market_daily(df_silver, calculated_at=fixed_calc_ts)
    expected_types = {
        "reference_date": DateType(),
        "ticker": StringType(),
        "close_price": DecimalType(18, 8),
        "daily_return": DecimalType(12, 6),
        "log_return": DecimalType(12, 6),
        "sma_7": DecimalType(18, 8),
        "sma_30": DecimalType(18, 8),
        "sma_200": DecimalType(18, 8),
        "rolling_vol_30d": DecimalType(12, 6),
        "rolling_vol_60d": DecimalType(12, 6),
        "rolling_vol_90d": DecimalType(12, 6),
        "real_interest_rate": DecimalType(10, 4),
        "selic_vs_us_spread": DecimalType(10, 4),
        "calculated_at": TimestampType(),
    }
    assert [(f.name, f.dataType) for f in df_gold.schema.fields] == list(expected_types.items())

    rows = {(r["ticker"], r["reference_date"]): r for r in df_gold.collect()}
    assert rows[("SELIC", date(2026, 1, 5))]["daily_return"] is None
    assert rows[("SELIC", date(2026, 1, 5))]["log_return"] is None
    assert float(rows[("USDBRL=X", date(2026, 1, 5))]["daily_return"]) == pytest.approx(0.10, abs=1e-6)
    assert float(rows[("USDBRL=X", date(2026, 1, 5))]["log_return"]) == pytest.approx(math.log(1.10), abs=1e-6)


def test_gold_cold_start_annualized_vol_and_calendar_gap_guard(spark: Any) -> None:
    """Verify cold-start nulls, sqrt(252) vs sqrt(365) vol scaling, and BTC-BRL calendar range guard."""
    from src.transforms.gold import compute_asset_analytics

    start_dt = date(2026, 1, 1)
    rows: list[tuple[date, str, float]] = []
    price = 100.0
    # 35 consecutive trading-style observations for USDBRL=X and 35 calendar observations for BTC-BRL
    for i in range(35):
        ref = start_dt + timedelta(days=i)
        if i > 0:
            price = price * (1.02 if i % 2 == 1 else 0.99)
        rows.append((ref, "USDBRL=X", round(price, 6)))
        rows.append((ref, "BTC-BRL", round(price, 6)))

    df_silver = spark.createDataFrame(rows, ["reference_date", "ticker", "close_price"])
    df_analytics = compute_asset_analytics(df_silver)

    usd_rows = df_analytics.filter(df_analytics.ticker == "USDBRL=X").orderBy("reference_date").collect()
    btc_rows = df_analytics.filter(df_analytics.ticker == "BTC-BRL").orderBy("reference_date").collect()

    # Cold start for SMA-7: indices 0..5 must be NULL, index 6 must be populated
    for idx in range(6):
        assert usd_rows[idx]["sma_7"] is None
        assert btc_rows[idx]["sma_7"] is None
    assert usd_rows[6]["sma_7"] is not None
    assert btc_rows[6]["sma_7"] is not None

    # Cold start for 30-return vol: index 29 has 29 returns -> NULL; index 30 has 30 returns -> non-NULL
    assert usd_rows[29]["rolling_vol_30d"] is None
    assert btc_rows[29]["rolling_vol_30d"] is None
    usd_vol = float(usd_rows[30]["rolling_vol_30d"])
    btc_vol = float(btc_rows[30]["rolling_vol_30d"])
    assert btc_vol / usd_vol == pytest.approx(math.sqrt(365.0) / math.sqrt(252.0), rel=1e-5)

    # Calendar range guard: removing 1 day from BTC-BRL prevents a 7-calendar-day window from reaching 7 obs
    df_gapped_btc = df_silver.filter((df_silver.ticker == "BTC-BRL") & (df_silver.reference_date != date(2026, 1, 4)))
    gapped_rows = compute_asset_analytics(df_gapped_btc).orderBy("reference_date").collect()
    # On 2026-01-07 (7th calendar day from 01-01), only 6 observations exist in [-6d, 0] -> sma_7 must stay NULL
    jan7 = next(r for r in gapped_rows if r["reference_date"] == date(2026, 1, 7))
    assert jan7["sma_7"] is None


def test_gold_fisher_real_interest_rate_spread_and_ipca_lag(spark: Any) -> None:
    """Verify daily SELIC compounding, 12m IPCA compounding, Fisher real rate, spread, and 40d IPCA lag."""
    from src.transforms.gold import compute_macro_indicators

    # Build 13 monthly IPCA prints from 2025-01-01 to 2026-01-01
    # First 12 months = 0.40%/month -> 12m compounded = (1.004^12 - 1)*100 = 4.9070207%
    # 13th month (2026-01-01) = 1.00%/month -> shifts 12m window once effective (2026-01-01 + 40d = 2026-02-10)
    data: list[tuple[date, str, float]] = []
    for month_idx in range(12):
        year = 2025
        month = month_idx + 1
        data.append((date(year, month, 1), "IPCA", 0.40))
    data.append((date(2026, 1, 1), "IPCA", 1.00))

    # Daily SELIC = 0.050788%/day (~13.65% p.a.), ^IRX = 4.35% p.a.
    # Dates evaluated: 2026-01-15 (before 2026-02-10 lag cutoff) and 2026-02-12 (after lag cutoff)
    for dt in (date(2026, 1, 15), date(2026, 2, 12)):
        data.append((dt, "SELIC", 0.050788))
        data.append((dt, "^IRX", 4.35))
        data.append((dt, "USDBRL=X", 5.50))

    df_silver = spark.createDataFrame(data, ["reference_date", "ticker", "close_price"])
    df_macro = compute_macro_indicators(df_silver, ipca_lag_days=40)
    by_date = {r["reference_date"]: r for r in df_macro.collect()}

    expected_selic_ann = ((1.0 + 0.050788 / 100.0) ** 252 - 1.0) * 100.0
    expected_ipca_jan = (1.004**12 - 1.0) * 100.0
    expected_real_jan = (((1.0 + expected_selic_ann / 100.0) / (1.0 + expected_ipca_jan / 100.0)) - 1.0) * 100.0

    # On 2026-01-15, 2026-01-01 IPCA (1.00%) must NOT leak yet; December 2025 12m IPCA (0.40% x 12) applies
    jan15 = by_date[date(2026, 1, 15)]
    assert jan15["real_interest_rate"] == pytest.approx(expected_real_jan, abs=1e-4)
    assert jan15["selic_vs_us_spread"] == pytest.approx(expected_selic_ann - 4.35, abs=1e-4)

    # On 2026-02-12 (>= 2026-01-01 + 40d = 2026-02-10), Jan 2026 IPCA (1.00%) is now active
    expected_ipca_feb = ((1.004**11) * 1.01 - 1.0) * 100.0
    expected_real_feb = (((1.0 + expected_selic_ann / 100.0) / (1.0 + expected_ipca_feb / 100.0)) - 1.0) * 100.0
    feb12 = by_date[date(2026, 2, 12)]
    assert feb12["real_interest_rate"] == pytest.approx(expected_real_feb, abs=1e-4)
    assert feb12["real_interest_rate"] < jan15["real_interest_rate"]
