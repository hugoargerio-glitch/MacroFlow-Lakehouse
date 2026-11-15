"""Pure, testable feature engineering and target construction utilities for MacroFlow FX forecasting."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

TARGET_HORIZONS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7)
AUTOREGRESSIVE_LAGS: tuple[int, ...] = (1, 3, 5, 15, 30)
EXOGENOUS_TICKERS: tuple[str, ...] = ("^BVSP", "BTC-BRL", "^IRX")

TARGET_COLUMNS: tuple[str, ...] = tuple(f"target_log_return_h{h}" for h in TARGET_HORIZONS)

FEATURE_COLUMNS: tuple[str, ...] = (
    # Contemporaneous spot state at t0 (known after market close on reference_date)
    "close_price",
    "daily_return",
    # Technical indicators & realized volatilities
    "rsi_14",
    "bb_spread_20",
    "ma_divergence_7_30",
    "rolling_vol_30d",
    "rolling_vol_60d",
    "rolling_vol_90d",
    # Macro & conformed exogenous signals (no lookahead)
    "real_interest_rate",
    "selic_vs_us_spread",
    "ibov_daily_return",
    "btc_daily_return",
    "irx_close_price",
    # Autoregressive lags across t-1, t-3, t-5, t-15, t-30
    "lag_close_1",
    "lag_return_1",
    "lag_vol_30d_1",
    "lag_close_3",
    "lag_return_3",
    "lag_vol_30d_3",
    "lag_close_5",
    "lag_return_5",
    "lag_vol_30d_5",
    "lag_close_15",
    "lag_return_15",
    "lag_vol_30d_15",
    "lag_close_30",
    "lag_return_30",
    "lag_vol_30d_30",
)

FEATURE_STORE_COLUMNS: tuple[str, ...] = (
    "reference_date",
    "ticker",
    *FEATURE_COLUMNS,
    *TARGET_COLUMNS,
)


def compute_wilder_rsi_series(close_prices: pd.Series, period: int = 14) -> pd.Series:
    """Compute Wilder's smoothed RSI(period) with warm-up NaNs and a 50.0 flat-price guard.

    Parameters
    ----------
    close_prices:
        Ordered closing price series.
    period:
        Lookback window for Wilder's smoothing (default 14).
    """
    if period <= 0:
        raise ValueError("RSI period must be positive.")

    prices = pd.to_numeric(close_prices, errors="coerce").astype(float)
    n = len(prices)
    rsi = np.full(n, np.nan, dtype=float)
    if n <= period:
        return pd.Series(rsi, index=close_prices.index, dtype=float)

    deltas = np.diff(prices.to_numpy(dtype=float))
    gains = np.where(deltas > 0.0, deltas, 0.0)
    losses = np.where(deltas < 0.0, -deltas, 0.0)

    # Require non-NaN seed window of `period` real price differences
    seed_gains = gains[:period]
    seed_losses = losses[:period]
    if np.isnan(deltas[:period]).any():
        return pd.Series(rsi, index=close_prices.index, dtype=float)

    avg_gain = float(np.mean(seed_gains))
    avg_loss = float(np.mean(seed_losses))

    def _rsi_from_avgs(ag: float, al: float) -> float:
        if ag == 0.0 and al == 0.0:
            return 50.0
        if al == 0.0:
            return 100.0
        rs = ag / al
        return 100.0 - (100.0 / (1.0 + rs))

    rsi[period] = _rsi_from_avgs(avg_gain, avg_loss)
    alpha_prev = float(period - 1)
    denom = float(period)

    for idx in range(period + 1, n):
        d = deltas[idx - 1]
        if np.isnan(d):
            rsi[idx] = np.nan
            continue
        g = d if d > 0.0 else 0.0
        l_val = -d if d < 0.0 else 0.0
        avg_gain = (avg_gain * alpha_prev + g) / denom
        avg_loss = (avg_loss * alpha_prev + l_val) / denom
        rsi[idx] = _rsi_from_avgs(avg_gain, avg_loss)

    return pd.Series(rsi, index=close_prices.index, dtype=float)


def compute_normalized_ma_divergence(sma_short: pd.Series, sma_long: pd.Series) -> pd.Series:
    """Compute dimensionless moving-average divergence ``(sma_short - sma_long) / sma_long``."""
    short = pd.to_numeric(sma_short, errors="coerce").astype(float)
    long = pd.to_numeric(sma_long, errors="coerce").astype(float)
    valid = long > 0.0
    result = pd.Series(np.nan, index=sma_short.index, dtype=float)
    result.loc[valid] = (short.loc[valid] - long.loc[valid]) / long.loc[valid]
    return result


def add_multi_horizon_targets(
    df: pd.DataFrame,
    price_col: str = "close_price",
    horizons: Sequence[int] = TARGET_HORIZONS,
) -> pd.DataFrame:
    """Attach stationary forward log-return targets ``ln(P_{t+h} / P_t)`` for each horizon ``h``."""
    out = df.copy()
    prices = pd.to_numeric(out[price_col], errors="coerce").astype(float)
    for h in horizons:
        if h <= 0:
            raise ValueError(f"Horizon must be positive, got {h}.")
        future_prices = prices.shift(-h)
        valid = (prices > 0.0) & (future_prices > 0.0)
        target = pd.Series(np.nan, index=out.index, dtype=float)
        target.loc[valid] = np.log(future_prices.loc[valid] / prices.loc[valid])
        out[f"target_log_return_h{h}"] = target
    return out


def build_fx_feature_store(
    df_gold: DataFrame,
    target_ticker: str = "USDBRL=X",
    rsi_period: int = 14,
    bb_window: int = 20,
    lags: Sequence[int] = AUTOREGRESSIVE_LAGS,
    horizons: Sequence[int] = TARGET_HORIZONS,
) -> DataFrame:
    """Transform ``macroflow_gold.fact_market_daily`` into a clean, leak-free Spark Feature Store."""
    import pyspark.sql.functions as F
    from pyspark.sql import Window

    w_time = Window.partitionBy("ticker").orderBy("reference_date")
    w_bb = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-(bb_window - 1), 0)
    # 6 * period historical differences keeps Wilder EMA truncation bias < 0.002
    wilder_lookback = rsi_period * 6 - 1
    w_wilder = Window.partitionBy("ticker").orderBy("reference_date").rowsBetween(-wilder_lookback, 0)

    df_fx = df_gold.filter(F.col("ticker") == target_ticker).withColumn("close_d", F.col("close_price").cast("double"))

    # 1. Guarded Wilder RSI(14) with flat-price 50.0 guard and cold-start NULLs
    df_rsi_prep = (
        df_fx.withColumn("delta_price", F.col("close_d") - F.lag("close_d", 1).over(w_time))
        .withColumn(
            "gain",
            F.when(F.col("delta_price").isNotNull(), F.greatest(F.col("delta_price"), F.lit(0.0))),
        )
        .withColumn(
            "loss",
            F.when(F.col("delta_price").isNotNull(), F.greatest(-F.col("delta_price"), F.lit(0.0))),
        )
        .withColumn("_gains_hist", F.collect_list("gain").over(w_wilder))
        .withColumn("_losses_hist", F.collect_list("loss").over(w_wilder))
    )

    alpha_prev = float(rsi_period - 1)
    period_f = float(rsi_period)

    def _wilder_ema(col_name: str):
        arr = F.col(col_name)
        seed = F.aggregate(F.slice(arr, 1, rsi_period), F.lit(0.0), lambda acc, x: acc + x) / F.lit(period_f)
        tail = F.slice(arr, rsi_period + 1, wilder_lookback + 1)
        return F.when(
            F.size(arr) >= rsi_period,
            F.aggregate(tail, seed, lambda acc, x: (acc * F.lit(alpha_prev) + x) / F.lit(period_f)),
        )

    df_rsi = (
        df_rsi_prep.withColumn("avg_gain_14", _wilder_ema("_gains_hist"))
        .withColumn("avg_loss_14", _wilder_ema("_losses_hist"))
        .withColumn(
            "rsi_14",
            F.when(F.col("avg_gain_14").isNull() | F.col("avg_loss_14").isNull(), F.lit(None).cast("double"))
            .when((F.col("avg_gain_14") == 0.0) & (F.col("avg_loss_14") == 0.0), F.lit(50.0))
            .when(F.col("avg_loss_14") == 0.0, F.lit(100.0))
            .otherwise(100.0 - (100.0 / (1.0 + (F.col("avg_gain_14") / F.col("avg_loss_14"))))),
        )
        .drop("delta_price", "gain", "loss", "_gains_hist", "_losses_hist", "avg_gain_14", "avg_loss_14")
    )

    # 2. Bollinger Band spread (full 20-obs warm-up) and normalized MA divergence
    df_technical = (
        df_rsi.withColumn("_bb_count", F.count("close_d").over(w_bb))
        .withColumn("bb_middle_20", F.avg("close_d").over(w_bb))
        .withColumn("bb_std_20", F.stddev("close_d").over(w_bb))
        .withColumn(
            "bb_spread_20",
            F.when(
                (F.col("_bb_count") == bb_window) & (F.col("bb_middle_20") > 0.0),
                (F.col("bb_std_20") * 4.0) / F.col("bb_middle_20"),
            ),
        )
        .withColumn(
            "ma_divergence_7_30",
            F.when(
                F.col("sma_30").cast("double") > 0.0,
                (F.col("sma_7").cast("double") - F.col("sma_30").cast("double")) / F.col("sma_30").cast("double"),
            ),
        )
        .drop("_bb_count", "bb_middle_20", "bb_std_20")
    )

    # 3. Autoregressive lags (t-1, t-3, t-5, t-15, t-30)
    df_lags = df_technical
    for lag_k in lags:
        df_lags = (
            df_lags.withColumn(f"lag_close_{lag_k}", F.lag("close_d", lag_k).over(w_time))
            .withColumn(f"lag_return_{lag_k}", F.lag(F.col("daily_return").cast("double"), lag_k).over(w_time))
            .withColumn(
                f"lag_vol_30d_{lag_k}",
                F.lag(F.col("rolling_vol_30d").cast("double"), lag_k).over(w_time),
            )
        )

    # 4. Direct Multi-Horizon Stationary Targets: ln(P_{t+h} / P_t) for h = 1..7
    df_targets = df_lags
    for h in horizons:
        future_close = F.lead("close_d", h).over(w_time)
        df_targets = df_targets.withColumn(
            f"target_log_return_h{h}",
            F.when((F.col("close_d") > 0.0) & (future_close > 0.0), F.log(future_close / F.col("close_d"))),
        )

    # 5. Lookahead-free Exogenous Alignment (^BVSP, BTC-BRL, ^IRX)
    df_exo_returns = (
        df_gold.filter(F.col("ticker").isin(["^BVSP", "BTC-BRL"]))
        .groupBy("reference_date")
        .pivot("ticker", ["^BVSP", "BTC-BRL"])
        .agg(F.first(F.col("daily_return").cast("double"), ignorenulls=True))
        .withColumnRenamed("^BVSP", "_ibov_ret_raw")
        .withColumnRenamed("BTC-BRL", "_btc_ret_raw")
    )

    # Forward-fill ^IRX yield level onto FX dates strictly <= reference_date (zero lookahead)
    w_irx_ffill = Window.partitionBy(F.lit(1)).orderBy("reference_date").rowsBetween(Window.unboundedPreceding, 0)
    df_irx_dates = (
        df_fx.select("reference_date")
        .unionByName(df_gold.filter(F.col("ticker") == "^IRX").select("reference_date"))
        .distinct()
    )
    df_irx_raw = df_gold.filter(F.col("ticker") == "^IRX").select(
        "reference_date", F.col("close_price").cast("double").alias("_irx_raw")
    )
    df_irx_aligned = (
        df_irx_dates.join(df_irx_raw, on="reference_date", how="left")
        .withColumn("irx_close_price", F.last("_irx_raw", ignorenulls=True).over(w_irx_ffill))
        .select("reference_date", "irx_close_price")
    )

    has_spread = "selic_vs_us_spread" in df_gold.columns
    spread_expr = (F.col("selic_vs_us_spread").cast("double") if has_spread else F.lit(0.0).cast("double")).alias(
        "selic_vs_us_spread"
    )

    df_joined = (
        df_targets.join(df_exo_returns, on="reference_date", how="left")
        .join(df_irx_aligned, on="reference_date", how="left")
        .withColumn("ibov_daily_return", F.coalesce(F.col("_ibov_ret_raw"), F.lit(0.0)))
        .withColumn("btc_daily_return", F.coalesce(F.col("_btc_ret_raw"), F.lit(0.0)))
        .withColumn("irx_close_price", F.coalesce(F.col("irx_close_price"), F.lit(0.0)))
    )

    # 6. Prune cold-start nulls cleanly (90d rolling vol + 30d lagged 30d vol + technical warm-ups)
    df_clean = df_joined.filter(
        F.col("close_d").isNotNull()
        & F.col("daily_return").isNotNull()
        & F.col("rsi_14").isNotNull()
        & F.col("bb_spread_20").isNotNull()
        & F.col("ma_divergence_7_30").isNotNull()
        & F.col("rolling_vol_30d").isNotNull()
        & F.col("rolling_vol_60d").isNotNull()
        & F.col("rolling_vol_90d").isNotNull()
        & F.col("lag_vol_30d_30").isNotNull()
    )

    target_selects = [F.col(f"target_log_return_h{h}").cast("double") for h in horizons]
    lag_selects = []
    for k in lags:
        lag_selects.extend(
            [
                F.col(f"lag_close_{k}").cast("double"),
                F.col(f"lag_return_{k}").cast("double"),
                F.col(f"lag_vol_30d_{k}").cast("double"),
            ]
        )

    return df_clean.select(
        F.col("reference_date"),
        F.col("ticker"),
        F.col("close_d").alias("close_price"),
        F.col("daily_return").cast("double").alias("daily_return"),
        F.col("rsi_14").cast("double"),
        F.col("bb_spread_20").cast("double"),
        F.col("ma_divergence_7_30").cast("double"),
        F.col("rolling_vol_30d").cast("double"),
        F.col("rolling_vol_60d").cast("double"),
        F.col("rolling_vol_90d").cast("double"),
        F.coalesce(F.col("real_interest_rate").cast("double"), F.lit(0.0)).alias("real_interest_rate"),
        spread_expr,
        F.col("ibov_daily_return").cast("double"),
        F.col("btc_daily_return").cast("double"),
        F.col("irx_close_price").cast("double"),
        *lag_selects,
        *target_selects,
        F.current_timestamp().alias("feature_timestamp"),
    )
