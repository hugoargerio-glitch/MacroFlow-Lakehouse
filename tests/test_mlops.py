"""Comprehensive unit tests for MacroFlow MLOps utilities in ``src/ml_utils``."""

from __future__ import annotations

import hashlib
import math
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.ml_utils import (
    FEATURE_COLUMNS,
    TARGET_COLUMNS,
    TARGET_HORIZONS,
    USDBRL_ASSET_ID,
    DirectMultiHorizonFXModel,
    HorizonArtifact,
    add_multi_horizon_targets,
    advance_b3_trading_days,
    build_forecast_id,
    build_forecast_records,
    build_fx_feature_store,
    build_snowflake_options,
    compute_conformal_quantile_adjustment,
    compute_easter_sunday,
    compute_horizon_metrics,
    compute_normalized_ma_divergence,
    compute_wilder_rsi_series,
    evaluate_direct_bundle,
    generate_forecast_merge_sql,
    generate_walk_forward_folds,
    get_forecast_spark_schema,
    is_b3_trading_day,
    make_train_val_test_split,
    reconstruct_price,
    run_walk_forward_cv,
    should_promote_challenger,
    validate_feature_freshness,
)


class _SpyLinearEstimator:
    """Lightweight numpy ridge-like regressor for testing without requiring LightGBM locally."""

    def __init__(self, objective: str = "regression", alpha: float = 0.5, **kwargs: Any) -> None:
        self.objective = objective
        self.alpha = alpha
        self.seen_indices: list[pd.Index] = []
        self.coef_: np.ndarray = np.array([])
        self.intercept_: float = 0.0
        self.quantile_shift_: float = 0.0

    def fit(self, x: pd.DataFrame, y: np.ndarray) -> _SpyLinearEstimator:
        self.seen_indices.append(x.index)
        x_mat = np.asarray(x, dtype=float)
        y_arr = np.asarray(y, dtype=float)
        # Regularized least squares for stable deterministic testing
        x_aug = np.column_stack([np.ones(len(x_mat)), x_mat])
        reg = np.eye(x_aug.shape[1]) * 1e-2
        reg[0, 0] = 0.0
        beta = np.linalg.solve(x_aug.T @ x_aug + reg, x_aug.T @ y_arr)
        self.intercept_ = float(beta[0])
        self.coef_ = beta[1:]
        residuals = y_arr - (self.intercept_ + x_mat @ self.coef_)
        if self.objective == "quantile":
            self.quantile_shift_ = float(np.quantile(residuals, self.alpha))
        else:
            self.quantile_shift_ = 0.0
        return self

    def predict(self, x: pd.DataFrame) -> np.ndarray:
        x_mat = np.asarray(x, dtype=float)
        return self.intercept_ + x_mat @ self.coef_ + self.quantile_shift_


def _make_synthetic_feature_df(n_rows: int = 240) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.bdate_range("2025-01-01", periods=n_rows + 10)
    returns = 0.0005 + 0.006 * rng.standard_normal(len(dates))
    prices = 5.0 * np.exp(np.cumsum(returns))

    df = pd.DataFrame({"reference_date": dates, "ticker": "USDBRL=X", "close_price": prices})
    df["daily_return"] = df["close_price"].pct_change().fillna(0.0)
    df["rsi_14"] = 50.0 + 10.0 * np.sin(np.arange(len(df)) / 7.0)
    df["bb_spread_20"] = 0.02 + 0.005 * np.cos(np.arange(len(df)) / 11.0)
    df["ma_divergence_7_30"] = 0.005 * rng.standard_normal(len(df))
    df["rolling_vol_30d"] = 0.12
    df["rolling_vol_60d"] = 0.13
    df["rolling_vol_90d"] = 0.14
    df["real_interest_rate"] = 6.5
    df["selic_vs_us_spread"] = 6.25
    df["ibov_daily_return"] = 0.004 * rng.standard_normal(len(df))
    df["btc_daily_return"] = 0.015 * rng.standard_normal(len(df))
    df["irx_close_price"] = 4.3

    for k in (1, 3, 5, 15, 30):
        df[f"lag_close_{k}"] = df["close_price"].shift(k).bfill()
        df[f"lag_return_{k}"] = df["daily_return"].shift(k).fillna(0.0)
        df[f"lag_vol_30d_{k}"] = 0.12

    df = add_multi_horizon_targets(df, price_col="close_price", horizons=TARGET_HORIZONS)
    return df.dropna(subset=list(TARGET_COLUMNS)).reset_index(drop=True)


def test_multi_horizon_targets_and_price_reconstruction() -> None:
    """Verify stationary log-return targets ln(P_{t+h}/P_t) and exact round-trip price reconstruction."""
    df = pd.DataFrame({"close_price": [5.0, 5.1, 5.2, 5.05, 5.25, 5.30, 5.35, 5.40]})
    out = add_multi_horizon_targets(df, horizons=(1, 2, 7))

    assert math.isclose(out.loc[0, "target_log_return_h1"], math.log(5.1 / 5.0), rel_tol=1e-12)
    assert math.isclose(out.loc[0, "target_log_return_h2"], math.log(5.2 / 5.0), rel_tol=1e-12)
    assert math.isclose(out.loc[0, "target_log_return_h7"], math.log(5.40 / 5.0), rel_tol=1e-12)
    assert pd.isna(out.loc[7, "target_log_return_h1"])

    reconstructed = reconstruct_price(5.0, float(out.loc[0, "target_log_return_h7"]))
    assert math.isclose(float(reconstructed), 5.40, rel_tol=1e-12)

    arr_reconstructed = reconstruct_price(np.array([5.0, 5.1]), np.array([0.0, math.log(5.2 / 5.1)]))
    assert np.allclose(arr_reconstructed, np.array([5.0, 5.2]))


def test_wilder_rsi_flat_price_guard_and_extreme_uptrend() -> None:
    """Verify Wilder RSI returns NaNs on warm-up, 50.0 on flat series, and 100.0 on monotonic gains."""
    flat = pd.Series([5.25] * 25)
    rsi_flat = compute_wilder_rsi_series(flat, period=14)
    assert rsi_flat.iloc[:14].isna().all()
    assert np.allclose(rsi_flat.iloc[14:], 50.0)

    uptrend = pd.Series([5.0 + 0.01 * i for i in range(25)])
    rsi_up = compute_wilder_rsi_series(uptrend, period=14)
    assert rsi_up.iloc[:14].isna().all()
    assert np.allclose(rsi_up.iloc[14:], 100.0)

    alternating = pd.Series([5.0 + (0.05 if i % 2 == 0 else -0.02) for i in range(30)])
    rsi_alt = compute_wilder_rsi_series(alternating, period=14)
    assert rsi_alt.iloc[14:].between(0.0, 100.0).all()

    div = compute_normalized_ma_divergence(pd.Series([5.5, 5.0]), pd.Series([5.0, 0.0]))
    assert math.isclose(float(div.iloc[0]), 0.1, rel_tol=1e-12)
    assert pd.isna(div.iloc[1])


def test_temporal_splits_and_zero_test_leakage() -> None:
    """Ensure train < val < test ordering and verify training/early-stopping never touches test rows."""
    split = make_train_val_test_split(n_samples=250, val_size=30, test_size=60, min_train_size=120)
    assert split.train == slice(0, 160)
    assert split.val == slice(160, 190)
    assert split.test == slice(190, 250)

    with pytest.raises(ValueError, match="Insufficient samples"):
        make_train_val_test_split(n_samples=100, val_size=30, test_size=60, min_train_size=120)

    folds = generate_walk_forward_folds(n_samples=240, n_splits=4, val_size=20, test_size=20, min_train_size=120)
    assert len(folds) == 4
    for fold in folds:
        assert fold.train.stop == fold.val.start
        assert fold.val.stop == fold.test.start

    # Verify SpyLinearEstimator never sees test indices during fit
    df = _make_synthetic_feature_df(240)
    split_df = make_train_val_test_split(len(df), val_size=30, test_size=60, min_train_size=120)
    created_estimators: list[_SpyLinearEstimator] = []

    def _spy_factory(**kwargs: Any) -> _SpyLinearEstimator:
        est = _SpyLinearEstimator(**kwargs)
        created_estimators.append(est)
        return est

    bundle, metrics = evaluate_direct_bundle(df, split_df, estimator_factory=_spy_factory)
    test_indices = set(df.index[split_df.test])
    for est in created_estimators:
        for seen_idx in est.seen_indices:
            assert test_indices.isdisjoint(set(seen_idx))

    assert "mean_rmse" in metrics
    assert "mean_naive_rmse" in metrics
    assert len(bundle.horizon_artifacts) == 7

    cv_summary = run_walk_forward_cv(df, n_splits=3, val_size=20, test_size=20, estimator_factory=_spy_factory)
    assert cv_summary["cv_mean_rmse"] > 0.0


def test_directional_accuracy_uses_spot_price_and_cqr_monotonicity() -> None:
    """Confirm directional_accuracy compares against spot P_t and CQR bounds maintain monotonicity."""
    spot = np.array([5.00, 5.00, 5.00, 5.00])
    y_true = np.array([5.10, 4.90, 5.05, 4.95])
    y_pred = np.array([5.08, 4.92, 4.99, 4.96])
    # Signs vs spot: actual=[+1,-1,+1,-1], pred=[+1,-1,-1,-1] -> 3/4 = 0.75
    metrics = compute_horizon_metrics(
        y_true_price=y_true,
        y_pred_price=y_pred,
        base_price=spot,
        lower_price=np.array([5.00, 4.85, 4.90, 4.90]),
        upper_price=np.array([5.15, 4.95, 5.10, 5.00]),
    )
    assert math.isclose(metrics["directional_accuracy"], 0.75)
    assert math.isclose(metrics["coverage_95"], 1.0)
    assert metrics["naive_rmse"] > 0.0

    # Test DirectMultiHorizonFXModel bound monotonicity even if quantile models cross point pred
    class _CrossedQuantileEstimator:
        def __init__(self, val: float) -> None:
            self.val = val

        def predict(self, x: pd.DataFrame) -> np.ndarray:
            return np.full(len(x), self.val, dtype=float)

    artifacts = {
        h: HorizonArtifact(
            horizon=h,
            point_model=_CrossedQuantileEstimator(0.01),
            lower_model=_CrossedQuantileEstimator(0.02),  # deliberately crossed above point
            upper_model=_CrossedQuantileEstimator(0.00),  # deliberately crossed below point
            conformal_adjustment=0.001,
        )
        for h in TARGET_HORIZONS
    }
    bundle = DirectMultiHorizonFXModel(horizon_artifacts=artifacts)
    df_in = _make_synthetic_feature_df(10).loc[[0], list(FEATURE_COLUMNS)]
    preds = bundle.predict(None, df_in)
    assert len(preds) == 7
    assert (preds["lower_log_return"] <= preds["pred_log_return"]).all()
    assert (preds["pred_log_return"] <= preds["upper_log_return"]).all()

    adj = compute_conformal_quantile_adjustment(
        y_val=np.array([0.0, 0.01, -0.02, 0.03]),
        q_low_val=np.array([-0.01, -0.01, -0.01, -0.01]),
        q_high_val=np.array([0.01, 0.01, 0.01, 0.01]),
    )
    assert adj >= 0.0


def test_champion_challenger_promotion_gate() -> None:
    """Verify promotion requires beating both naive random walk and existing @champion."""
    promote_fail_naive, _ = should_promote_challenger(
        challenger_metrics={"mean_rmse": 0.050, "mean_naive_rmse": 0.045},
        champion_metrics=None,
    )
    assert not promote_fail_naive

    promote_first_win, _ = should_promote_challenger(
        challenger_metrics={"mean_rmse": 0.040, "mean_naive_rmse": 0.045},
        champion_metrics=None,
    )
    assert promote_first_win

    promote_worse_than_champ, _ = should_promote_challenger(
        challenger_metrics={"mean_rmse": 0.042, "mean_naive_rmse": 0.045},
        champion_metrics={"mean_rmse": 0.038},
    )
    assert not promote_worse_than_champ

    promote_better_than_champ, _ = should_promote_challenger(
        challenger_metrics={"mean_rmse": 0.035, "mean_naive_rmse": 0.045},
        champion_metrics={"mean_rmse": 0.038},
    )
    assert promote_better_than_champ


def test_b3_trading_calendar_stepping_and_holidays() -> None:
    """Ensure horizon dates skip weekends, fixed B3 holidays, and movable Easter-linked holidays."""
    assert compute_easter_sunday(2026) == date(2026, 4, 5)
    assert not is_b3_trading_day(date(2026, 2, 16))  # Carnaval Monday
    assert not is_b3_trading_day(date(2026, 2, 17))  # Carnaval Tuesday
    assert not is_b3_trading_day(date(2026, 4, 3))  # Good Friday
    assert not is_b3_trading_day(date(2026, 6, 4))  # Corpus Christi
    assert not is_b3_trading_day(date(2026, 12, 24))  # Christmas Eve (B3 closed)
    assert not is_b3_trading_day(date(2026, 12, 31))  # Year-end bank holiday (B3 closed)
    assert is_b3_trading_day(date(2026, 11, 16))  # Monday after Nov 15

    # Friday before Carnaval 2026 (2026-02-13) -> next B3 trading day is Ash Wednesday (2026-02-18)
    horizons = advance_b3_trading_days(date(2026, 2, 13), steps=7)
    assert len(horizons) == 7
    assert horizons[0] == date(2026, 2, 18)
    assert all(is_b3_trading_day(d) for d in horizons)


def test_inference_records_freshness_md5_and_snowflake_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify freshness guards, MD5 forecast IDs, bound monotonicity, Snowflake options, and MERGE SQL."""
    ref_date = date(2026, 11, 13)
    validate_feature_freshness(ref_date, date(2026, 11, 15), max_staleness_days=5)
    with pytest.raises(ValueError, match="stale"):
        validate_feature_freshness(ref_date, date(2026, 11, 25), max_staleness_days=5)
    with pytest.raises(ValueError, match="exceeds"):
        validate_feature_freshness(date(2026, 11, 16), date(2026, 11, 15))

    preds_df = pd.DataFrame(
        {
            "horizon_days": list(range(1, 8)),
            "pred_log_return": [0.001 * h for h in range(1, 8)],
            "lower_log_return": [-0.005 * h for h in range(1, 8)],
            "upper_log_return": [0.007 * h for h in range(1, 8)],
        }
    )
    records = build_forecast_records(
        reference_date=ref_date,
        current_price=5.8000,
        predictions_df=preds_df,
        model_version="LightGBM_Direct_v3",
    )
    assert len(records) == 7
    for rec in records:
        assert rec["ASSET_ID"] == USDBRL_ASSET_ID
        assert isinstance(rec["PREDICTED_VALUE"], Decimal)
        assert rec["LOWER_BOUND_95"] <= rec["PREDICTED_VALUE"] <= rec["UPPER_BOUND_95"]
        expected_id = hashlib.md5(
            f"{ref_date.isoformat()}_{rec['TARGET_HORIZON_DATE'].isoformat()}_1_LightGBM_Direct_v3".encode("utf-8")
        ).hexdigest()
        assert rec["FORECAST_ID"] == expected_id
        assert build_forecast_id(ref_date, rec["TARGET_HORIZON_DATE"], 1, "LightGBM_Direct_v3") == expected_id

    # Missing secrets rejected
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_URL", raising=False)
    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        build_snowflake_options()

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
    monkeypatch.setenv("SNOWFLAKE_USER", "macroflow_svc")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "secret")
    opts = build_snowflake_options()
    assert opts["sfURL"] == "https://xy12345.us-east-1.snowflakecomputing.com"
    assert opts["sfRole"] == "MACROFLOW_ETL_ROLE"

    merge_sql = generate_forecast_merge_sql()
    assert "MERGE INTO MACROFLOW_DW.GOLD.FACT_MACRO_FORECASTS AS target" in merge_sql
    assert "ON target.forecast_id = source.forecast_id" in merge_sql
    assert "TRUNCATE TABLE IF EXISTS MACROFLOW_DW.GOLD.STG_FACT_MACRO_FORECASTS" in merge_sql


def test_pyspark_feature_store_and_forecast_schema_if_available() -> None:
    """Run real PySpark feature store & forecast schema tests when pyspark is installed (CI)."""
    pyspark = pytest.importorskip("pyspark")
    assert pyspark is not None
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").appName("MacroFlow-ML-Tests").getOrCreate()
    try:
        schema = get_forecast_spark_schema()
        assert schema is not None
        assert len(schema.fields) == 9

        # Build 150 synthetic Gold rows across USDBRL=X, ^BVSP, BTC-BRL, ^IRX
        start = date(2025, 1, 1)
        rows = []
        for i in range(150):
            d = start + timedelta(days=i)
            price = 5.0 + 0.005 * i + 0.05 * math.sin(i / 4.0)
            rows.append((d, "USDBRL=X", price, 0.001, 5.0, 4.95, 0.12, 0.13, 0.14, 6.5, 6.2))
            if d.weekday() < 5:
                rows.append((d, "^BVSP", 120000.0 + i * 10.0, 0.002, 120000.0, 119500.0, 0.15, 0.16, 0.17, 6.5, 6.2))
                rows.append((d, "^IRX", 4.3, 0.0, 4.3, 4.3, 0.01, 0.01, 0.01, 6.5, 6.2))
            rows.append((d, "BTC-BRL", 350000.0 + i * 50.0, 0.01, 350000.0, 345000.0, 0.45, 0.45, 0.45, 6.5, 6.2))

        cols = [
            "reference_date",
            "ticker",
            "close_price",
            "daily_return",
            "sma_7",
            "sma_30",
            "rolling_vol_30d",
            "rolling_vol_60d",
            "rolling_vol_90d",
            "real_interest_rate",
            "selic_vs_us_spread",
        ]
        df_gold = spark.createDataFrame(rows, schema=cols)
        df_fs = build_fx_feature_store(df_gold)
        pdf = df_fs.toPandas()
        assert len(pdf) > 0
        for col_name in FEATURE_COLUMNS:
            assert col_name in pdf.columns
            assert not pdf[col_name].isna().any(), f"Unexpected NaNs in {col_name}"
        for target_col in TARGET_COLUMNS:
            assert target_col in pdf.columns
    finally:
        spark.stop()
