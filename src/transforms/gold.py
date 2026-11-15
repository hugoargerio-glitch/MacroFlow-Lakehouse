"""Pure PySpark analytical transformations and Delta MERGE logic for MacroFlow Gold layer."""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import DateType, DecimalType, StringType, TimestampType

from src.transforms.silver import DEFAULT_SILVER_TABLE

logger = logging.getLogger(__name__)

DEFAULT_GOLD_TABLE = "macroflow_gold.fact_market_daily"

TRADING_MARKET_TICKERS: tuple[str, ...] = ("USDBRL=X", "^BVSP", "USD_PTAX")
CALENDAR_24_7_TICKERS: tuple[str, ...] = ("BTC-BRL",)
MACRO_RATE_TICKERS: tuple[str, ...] = ("SELIC", "IPCA", "^IRX")

TRADING_DAYS_PER_YEAR = 252
CALENDAR_DAYS_PER_YEAR = 365
DEFAULT_IPCA_PUBLICATION_LAG_DAYS = 40

GOLD_PRICE_DECIMAL = DecimalType(18, 8)
GOLD_METRIC_DECIMAL = DecimalType(12, 6)
GOLD_RATE_DECIMAL = DecimalType(10, 4)


def _compute_trading_day_asset_metrics(df_trading: DataFrame) -> DataFrame:
    """Compute returns, trading-day SMAs, and annualized (252d) volatilities for market-hours assets."""
    w_lag = Window.partitionBy("ticker").orderBy("reference_date")
    w_7 = w_lag.rowsBetween(-6, 0)
    w_30 = w_lag.rowsBetween(-29, 0)
    w_60 = w_lag.rowsBetween(-59, 0)
    w_90 = w_lag.rowsBetween(-89, 0)
    w_200 = w_lag.rowsBetween(-199, 0)

    ann_factor = math.sqrt(TRADING_DAYS_PER_YEAR)

    return (
        df_trading.withColumn("_close_dbl", F.col("close_price").cast("double"))
        .withColumn("_prev_close", F.lag("_close_dbl", 1).over(w_lag))
        .withColumn(
            "daily_return",
            F.when(F.col("_prev_close") > 0.0, (F.col("_close_dbl") - F.col("_prev_close")) / F.col("_prev_close")),
        )
        .withColumn(
            "log_return",
            F.when(
                (F.col("_prev_close") > 0.0) & (F.col("_close_dbl") > 0.0),
                F.log(F.col("_close_dbl") / F.col("_prev_close")),
            ),
        )
        .withColumn("sma_7", F.when(F.count("_close_dbl").over(w_7) == 7, F.avg("_close_dbl").over(w_7)))
        .withColumn("sma_30", F.when(F.count("_close_dbl").over(w_30) == 30, F.avg("_close_dbl").over(w_30)))
        .withColumn("sma_200", F.when(F.count("_close_dbl").over(w_200) == 200, F.avg("_close_dbl").over(w_200)))
        .withColumn(
            "rolling_vol_30d",
            F.when(
                F.count("daily_return").over(w_30) >= 30,
                F.stddev("daily_return").over(w_30) * F.lit(ann_factor),
            ),
        )
        .withColumn(
            "rolling_vol_60d",
            F.when(
                F.count("daily_return").over(w_60) >= 60,
                F.stddev("daily_return").over(w_60) * F.lit(ann_factor),
            ),
        )
        .withColumn(
            "rolling_vol_90d",
            F.when(
                F.count("daily_return").over(w_90) >= 90,
                F.stddev("daily_return").over(w_90) * F.lit(ann_factor),
            ),
        )
        .select(
            "reference_date",
            "ticker",
            "close_price",
            "daily_return",
            "log_return",
            "sma_7",
            "sma_30",
            "sma_200",
            "rolling_vol_30d",
            "rolling_vol_60d",
            "rolling_vol_90d",
        )
    )


def _compute_calendar_24_7_asset_metrics(df_crypto: DataFrame) -> DataFrame:
    """Compute returns, calendar-day ``rangeBetween`` SMAs, and annualized (365d) volatilities for 24/7 assets."""
    w_lag = Window.partitionBy("ticker").orderBy("reference_date")
    w_cal_base = Window.partitionBy("ticker").orderBy("_ts_sec")

    seconds_per_day = 86400
    w_7 = w_cal_base.rangeBetween(-6 * seconds_per_day, 0)
    w_30 = w_cal_base.rangeBetween(-29 * seconds_per_day, 0)
    w_60 = w_cal_base.rangeBetween(-59 * seconds_per_day, 0)
    w_90 = w_cal_base.rangeBetween(-89 * seconds_per_day, 0)
    w_200 = w_cal_base.rangeBetween(-199 * seconds_per_day, 0)

    ann_factor = math.sqrt(CALENDAR_DAYS_PER_YEAR)

    return (
        df_crypto.withColumn("_close_dbl", F.col("close_price").cast("double"))
        .withColumn("_ts_sec", F.unix_timestamp(F.col("reference_date").cast("timestamp")))
        .withColumn("_prev_close", F.lag("_close_dbl", 1).over(w_lag))
        .withColumn(
            "daily_return",
            F.when(F.col("_prev_close") > 0.0, (F.col("_close_dbl") - F.col("_prev_close")) / F.col("_prev_close")),
        )
        .withColumn(
            "log_return",
            F.when(
                (F.col("_prev_close") > 0.0) & (F.col("_close_dbl") > 0.0),
                F.log(F.col("_close_dbl") / F.col("_prev_close")),
            ),
        )
        .withColumn("sma_7", F.when(F.count("_close_dbl").over(w_7) == 7, F.avg("_close_dbl").over(w_7)))
        .withColumn("sma_30", F.when(F.count("_close_dbl").over(w_30) == 30, F.avg("_close_dbl").over(w_30)))
        .withColumn("sma_200", F.when(F.count("_close_dbl").over(w_200) == 200, F.avg("_close_dbl").over(w_200)))
        .withColumn(
            "rolling_vol_30d",
            F.when(
                F.count("daily_return").over(w_30) >= 30,
                F.stddev("daily_return").over(w_30) * F.lit(ann_factor),
            ),
        )
        .withColumn(
            "rolling_vol_60d",
            F.when(
                F.count("daily_return").over(w_60) >= 60,
                F.stddev("daily_return").over(w_60) * F.lit(ann_factor),
            ),
        )
        .withColumn(
            "rolling_vol_90d",
            F.when(
                F.count("daily_return").over(w_90) >= 90,
                F.stddev("daily_return").over(w_90) * F.lit(ann_factor),
            ),
        )
        .select(
            "reference_date",
            "ticker",
            "close_price",
            "daily_return",
            "log_return",
            "sma_7",
            "sma_30",
            "sma_200",
            "rolling_vol_30d",
            "rolling_vol_60d",
            "rolling_vol_90d",
        )
    )


def _format_macro_rate_series(df_macro: DataFrame) -> DataFrame:
    """Keep macro rate series with NULL return/SMA/volatility columns to prevent invalid rate-on-rate math."""
    null_dbl = F.lit(None).cast("double")
    return df_macro.select(
        "reference_date",
        "ticker",
        "close_price",
        null_dbl.alias("daily_return"),
        null_dbl.alias("log_return"),
        null_dbl.alias("sma_7"),
        null_dbl.alias("sma_30"),
        null_dbl.alias("sma_200"),
        null_dbl.alias("rolling_vol_30d"),
        null_dbl.alias("rolling_vol_60d"),
        null_dbl.alias("rolling_vol_90d"),
    )


def compute_asset_analytics(df_silver: DataFrame) -> DataFrame:
    """Compute asset-level analytics while separating trading, 24/7 calendar, and macro rate series."""
    df_trading = _compute_trading_day_asset_metrics(df_silver.filter(F.col("ticker").isin(*TRADING_MARKET_TICKERS)))
    df_crypto = _compute_calendar_24_7_asset_metrics(df_silver.filter(F.col("ticker").isin(*CALENDAR_24_7_TICKERS)))
    df_macro = _format_macro_rate_series(
        df_silver.filter(~F.col("ticker").isin(*(TRADING_MARKET_TICKERS + CALENDAR_24_7_TICKERS)))
    )
    return df_trading.unionByName(df_crypto).unionByName(df_macro)


def compute_macro_indicators(
    df_silver: DataFrame,
    *,
    ipca_lag_days: int = DEFAULT_IPCA_PUBLICATION_LAG_DAYS,
) -> DataFrame:
    """Compute annualized Fisher ``real_interest_rate`` and ``selic_vs_us_spread`` without lookahead bias.

    Conversions applied before Fisher / spread equations:
    - ``SELIC`` (BCB SGS 11, %/day): compounded to % p.a. via ``((1 + r_d / 100)^252 - 1) * 100``.
    - ``IPCA`` (BCB SGS 433, %/month): compounded over rolling 12 months via ``expm1(sum(log1p(r_m / 100))) * 100``
      and shifted forward by ``ipca_lag_days`` (default 40 days from reference month start) to reflect release lag.
    - ``^IRX`` (Yahoo Finance, % p.a.): already expressed as annualized percentage points.
    """
    df_market_dates = df_silver.select("reference_date").distinct()

    df_selic = df_silver.filter(F.col("ticker") == "SELIC").select(
        "reference_date",
        (
            (
                F.pow(
                    F.lit(1.0) + (F.col("close_price").cast("double") / F.lit(100.0)),
                    F.lit(float(TRADING_DAYS_PER_YEAR)),
                )
                - F.lit(1.0)
            )
            * F.lit(100.0)
        ).alias("selic_ann_pct"),
    )

    df_ipca_raw = df_silver.filter(F.col("ticker") == "IPCA").select(
        "reference_date",
        F.col("close_price").cast("double").alias("ipca_mom_pct"),
    )
    w_ipca_12m = Window.partitionBy(F.lit(1)).orderBy("reference_date").rowsBetween(-11, 0)
    w_ipca_eff = Window.partitionBy("effective_date").orderBy(F.col("ref_month_date").desc())

    df_ipca_effective = (
        df_ipca_raw.filter(F.col("ipca_mom_pct") > F.lit(-100.0))
        .withColumn("_ipca_count", F.count("ipca_mom_pct").over(w_ipca_12m))
        .withColumn(
            "ipca_12m_pct",
            F.when(
                F.col("_ipca_count") == 12,
                F.expm1(F.sum(F.log1p(F.col("ipca_mom_pct") / F.lit(100.0))).over(w_ipca_12m)) * F.lit(100.0),
            ),
        )
        .filter(F.col("ipca_12m_pct").isNotNull())
        .withColumn("effective_date", F.date_add(F.col("reference_date"), ipca_lag_days))
        .withColumn("ref_month_date", F.col("reference_date"))
        .withColumn("_rn", F.row_number().over(w_ipca_eff))
        .filter(F.col("_rn") == 1)
        .select(F.col("effective_date").alias("reference_date"), "ipca_12m_pct")
    )

    df_irx = df_silver.filter(F.col("ticker") == "^IRX").select(
        "reference_date",
        F.col("close_price").cast("double").alias("irx_ann_pct"),
    )

    df_spine = df_market_dates.unionByName(df_ipca_effective.select("reference_date")).distinct()
    w_cal = Window.partitionBy(F.lit(1)).orderBy("reference_date").rowsBetween(Window.unboundedPreceding, 0)

    df_macro_filled = (
        df_spine.join(df_selic, on="reference_date", how="left")
        .join(df_ipca_effective, on="reference_date", how="left")
        .join(df_irx, on="reference_date", how="left")
        .withColumn("selic_filled", F.last("selic_ann_pct", ignorenulls=True).over(w_cal))
        .withColumn("ipca_filled", F.last("ipca_12m_pct", ignorenulls=True).over(w_cal))
        .withColumn("irx_filled", F.last("irx_ann_pct", ignorenulls=True).over(w_cal))
        .withColumn(
            "real_interest_rate",
            F.when(
                F.col("selic_filled").isNotNull()
                & F.col("ipca_filled").isNotNull()
                & ((F.lit(1.0) + F.col("ipca_filled") / F.lit(100.0)) > F.lit(0.0)),
                (
                    (
                        (F.lit(1.0) + F.col("selic_filled") / F.lit(100.0))
                        / (F.lit(1.0) + F.col("ipca_filled") / F.lit(100.0))
                    )
                    - F.lit(1.0)
                )
                * F.lit(100.0),
            ),
        )
        .withColumn(
            "selic_vs_us_spread",
            F.when(
                F.col("selic_filled").isNotNull() & F.col("irx_filled").isNotNull(),
                F.col("selic_filled") - F.col("irx_filled"),
            ),
        )
    )

    return df_macro_filled.join(df_market_dates, on="reference_date", how="left_semi").select(
        "reference_date",
        "real_interest_rate",
        "selic_vs_us_spread",
    )


def build_gold_fact_market_daily(
    df_silver: DataFrame,
    *,
    ipca_lag_days: int = DEFAULT_IPCA_PUBLICATION_LAG_DAYS,
    calculated_at: Optional[datetime] = None,
) -> DataFrame:
    """Assemble ``macroflow_gold.fact_market_daily`` conforming strictly to the Snowflake contract."""
    df_assets = compute_asset_analytics(df_silver)
    df_macro = compute_macro_indicators(df_silver, ipca_lag_days=ipca_lag_days)
    ts_expr = F.lit(calculated_at).cast(TimestampType()) if calculated_at is not None else F.current_timestamp()

    return df_assets.join(df_macro, on="reference_date", how="left").select(
        F.col("reference_date").cast(DateType()).alias("reference_date"),
        F.col("ticker").cast(StringType()).alias("ticker"),
        F.col("close_price").cast(GOLD_PRICE_DECIMAL).alias("close_price"),
        F.col("daily_return").cast(GOLD_METRIC_DECIMAL).alias("daily_return"),
        F.col("log_return").cast(GOLD_METRIC_DECIMAL).alias("log_return"),
        F.col("sma_7").cast(GOLD_PRICE_DECIMAL).alias("sma_7"),
        F.col("sma_30").cast(GOLD_PRICE_DECIMAL).alias("sma_30"),
        F.col("sma_200").cast(GOLD_PRICE_DECIMAL).alias("sma_200"),
        F.col("rolling_vol_30d").cast(GOLD_METRIC_DECIMAL).alias("rolling_vol_30d"),
        F.col("rolling_vol_60d").cast(GOLD_METRIC_DECIMAL).alias("rolling_vol_60d"),
        F.col("rolling_vol_90d").cast(GOLD_METRIC_DECIMAL).alias("rolling_vol_90d"),
        F.col("real_interest_rate").cast(GOLD_RATE_DECIMAL).alias("real_interest_rate"),
        F.col("selic_vs_us_spread").cast(GOLD_RATE_DECIMAL).alias("selic_vs_us_spread"),
        ts_expr.alias("calculated_at"),
    )


def _ensure_schema_exists(spark: SparkSession, table_name: str) -> None:
    if "." in table_name:
        schema_name = table_name.rsplit(".", 1)[0]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def merge_gold_fact_market_daily(
    spark: SparkSession,
    df_gold: DataFrame,
    target_table: str = DEFAULT_GOLD_TABLE,
    *,
    lookback_days: Optional[int] = None,
) -> None:
    """Persist Gold analytical rows via Liquid Clustering and a change-guarded Delta MERGE."""
    if not spark.catalog.tableExists(target_table):
        _ensure_schema_exists(spark, target_table)
        try:
            (df_gold.writeTo(target_table).using("delta").clusterBy("ticker", "reference_date").createOrReplace())
        except Exception:  # noqa: BLE001
            logger.info("Falling back to unpartitioned Delta creation + ZORDER for %s.", target_table)
            df_gold.write.format("delta").mode("overwrite").saveAsTable(target_table)
            try:
                spark.sql(f"OPTIMIZE {target_table} ZORDER BY (ticker, reference_date)")
            except Exception:  # noqa: BLE001
                pass
        return

    df_to_merge = df_gold
    if lookback_days is not None and lookback_days >= 0:
        max_date = df_gold.agg(F.max("reference_date").alias("max_date")).collect()[0]["max_date"]
        if max_date is not None:
            df_to_merge = df_gold.filter(
                F.col("reference_date") >= F.date_sub(F.lit(max_date.isoformat()).cast(DateType()), lookback_days)
            )

    min_date = df_to_merge.agg(F.min("reference_date").alias("min_date")).collect()[0]["min_date"]
    if min_date is None:
        logger.info("No Gold records to merge into %s.", target_table)
        return

    merge_condition = (
        "target.reference_date = source.reference_date "
        "AND target.ticker = source.ticker "
        f"AND target.reference_date >= DATE '{min_date.isoformat()}'"
    )
    change_predicate = (
        "NOT (target.close_price <=> source.close_price) "
        "OR NOT (target.daily_return <=> source.daily_return) "
        "OR NOT (target.log_return <=> source.log_return) "
        "OR NOT (target.sma_7 <=> source.sma_7) "
        "OR NOT (target.sma_30 <=> source.sma_30) "
        "OR NOT (target.sma_200 <=> source.sma_200) "
        "OR NOT (target.rolling_vol_30d <=> source.rolling_vol_30d) "
        "OR NOT (target.rolling_vol_60d <=> source.rolling_vol_60d) "
        "OR NOT (target.rolling_vol_90d <=> source.rolling_vol_90d) "
        "OR NOT (target.real_interest_rate <=> source.real_interest_rate) "
        "OR NOT (target.selic_vs_us_spread <=> source.selic_vs_us_spread)"
    )

    delta_gold = DeltaTable.forName(spark, target_table)
    (
        delta_gold.alias("target")
        .merge(source=df_to_merge.alias("source"), condition=merge_condition)
        .whenMatchedUpdateAll(condition=change_predicate)
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_gold_analytics(
    spark: SparkSession,
    *,
    silver_table: str = DEFAULT_SILVER_TABLE,
    target_table: str = DEFAULT_GOLD_TABLE,
    ipca_lag_days: int = DEFAULT_IPCA_PUBLICATION_LAG_DAYS,
    lookback_days: Optional[int] = None,
) -> DataFrame:
    """Read Silver conformed data, compute Gold analytical metrics, and merge into Delta."""
    df_silver = spark.read.table(silver_table)
    df_gold = build_gold_fact_market_daily(df_silver, ipca_lag_days=ipca_lag_days)
    merge_gold_fact_market_daily(spark, df_gold, target_table=target_table, lookback_days=lookback_days)
    return df_gold
