"""Walk-forward validation, direct multi-horizon training, conformal calibration, and promotion logic."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

from src.ml_utils.features import FEATURE_COLUMNS, TARGET_HORIZONS

try:
    import mlflow.pyfunc as _mlflow_pyfunc

    _PythonModelBase: type[Any] = _mlflow_pyfunc.PythonModel
except ImportError:  # Allow pure-Python unit testing when mlflow is absent

    class _PythonModelBaseFallback:
        pass

    _PythonModelBase = _PythonModelBaseFallback


MODEL_REGISTRY_NAME = "MacroFlow_USDBRL_Forecaster"

DEFAULT_LGBM_PARAMS: dict[str, Any] = {
    "n_estimators": 400,
    "learning_rate": 0.03,
    "max_depth": 5,
    "num_leaves": 31,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_samples": 20,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "random_state": 42,
    "verbose": -1,
}


@dataclass(frozen=True)
class TemporalSplit:
    """Strictly ordered temporal index slices (``train < val < test``) with zero overlap."""

    train: slice
    val: slice
    test: slice

    def __post_init__(self) -> None:
        if self.train.stop > self.val.start or self.val.stop > self.test.start:
            raise ValueError(f"Temporal leakage detected in split: {self}")


@dataclass
class HorizonArtifact:
    """Fitted point + quantile models and conformal quantile calibration for one horizon."""

    horizon: int
    point_model: Any
    lower_model: Any
    upper_model: Any
    conformal_adjustment: float


def make_train_val_test_split(
    n_samples: int,
    val_size: int = 30,
    test_size: int = 60,
    min_train_size: int = 120,
) -> TemporalSplit:
    """Construct a strict 3-way temporal split where ``test`` is never seen during early stopping."""
    if val_size <= 0 or test_size <= 0 or min_train_size <= 0:
        raise ValueError("val_size, test_size, and min_train_size must be positive.")
    required = min_train_size + val_size + test_size
    if n_samples < required:
        raise ValueError(f"Insufficient samples ({n_samples}) for temporal split; require at least {required}.")
    val_start = n_samples - test_size - val_size
    test_start = n_samples - test_size
    return TemporalSplit(
        train=slice(0, val_start),
        val=slice(val_start, test_start),
        test=slice(test_start, n_samples),
    )


def generate_walk_forward_folds(
    n_samples: int,
    n_splits: int = 4,
    val_size: int = 20,
    test_size: int = 20,
    min_train_size: int = 120,
) -> list[TemporalSplit]:
    """Generate expanding-window walk-forward folds across historical time regimes."""
    if n_splits <= 0:
        raise ValueError("n_splits must be positive.")
    required = min_train_size + val_size + n_splits * test_size
    if n_samples < required:
        raise ValueError(f"Insufficient samples ({n_samples}) for {n_splits} walk-forward folds (need {required}).")

    folds: list[TemporalSplit] = []
    first_test_start = n_samples - n_splits * test_size
    for fold_idx in range(n_splits):
        test_start = first_test_start + fold_idx * test_size
        test_stop = test_start + test_size
        val_start = test_start - val_size
        folds.append(
            TemporalSplit(
                train=slice(0, val_start),
                val=slice(val_start, test_start),
                test=slice(test_start, test_stop),
            )
        )
    return folds


def reconstruct_price(base_price: np.ndarray | float, log_return: np.ndarray | float) -> np.ndarray | float:
    """Reconstruct projected FX price from spot price ``P_t`` and log-return ``r_{t,h}``."""
    if isinstance(base_price, (int, float)) and isinstance(log_return, (int, float)):
        return float(base_price * math.exp(log_return))
    return np.asarray(base_price, dtype=float) * np.exp(np.asarray(log_return, dtype=float))


def compute_conformal_quantile_adjustment(
    y_val: np.ndarray,
    q_low_val: np.ndarray,
    q_high_val: np.ndarray,
    alpha: float = 0.05,
) -> float:
    """Compute finite-sample Conformalized Quantile Regression (CQR) adjustment on validation residuals."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must lie strictly in (0, 1).")
    y_arr = np.asarray(y_val, dtype=float)
    low_arr = np.asarray(q_low_val, dtype=float)
    high_arr = np.asarray(q_high_val, dtype=float)
    n = len(y_arr)
    if n == 0:
        raise ValueError("Validation residuals cannot be empty for conformal calibration.")

    scores = np.maximum(low_arr - y_arr, y_arr - high_arr)
    level = min(1.0, math.ceil((1.0 - alpha) * (n + 1)) / n)
    q_adj = float(np.quantile(scores, level, method="higher"))
    return max(0.0, q_adj)


def compute_horizon_metrics(
    y_true_price: np.ndarray,
    y_pred_price: np.ndarray,
    base_price: np.ndarray,
    lower_price: Optional[np.ndarray] = None,
    upper_price: Optional[np.ndarray] = None,
) -> dict[str, float]:
    """Compute price-space accuracy, 1-step-from-spot directional accuracy, and naive baseline skill."""
    y_true = np.asarray(y_true_price, dtype=float)
    y_pred = np.asarray(y_pred_price, dtype=float)
    spot = np.asarray(base_price, dtype=float)

    if len(y_true) == 0:
        raise ValueError("Evaluation arrays cannot be empty.")

    errors = y_true - y_pred
    rmse = float(np.sqrt(np.mean(errors**2)))
    mae = float(np.mean(np.abs(errors)))

    pos_mask = y_true > 0.0
    mape = float(np.mean(np.abs(errors[pos_mask]) / y_true[pos_mask])) if pos_mask.any() else float("nan")

    # Direction strictly relative to P_t (`close_price`), never lag_close_1
    actual_dir = np.sign(y_true - spot)
    pred_dir = np.sign(y_pred - spot)
    directional_accuracy = float(np.mean(actual_dir == pred_dir))

    # Naive random-walk baseline: P_hat_{t+h} = P_t
    naive_errors = y_true - spot
    naive_rmse = float(np.sqrt(np.mean(naive_errors**2)))
    naive_mae = float(np.mean(np.abs(naive_errors)))
    skill_vs_naive = float(1.0 - (rmse / naive_rmse)) if naive_rmse > 0.0 else 0.0

    metrics: dict[str, float] = {
        "rmse": rmse,
        "mae": mae,
        "mape": mape,
        "directional_accuracy": directional_accuracy,
        "naive_rmse": naive_rmse,
        "naive_mae": naive_mae,
        "skill_vs_naive": skill_vs_naive,
    }

    if lower_price is not None and upper_price is not None:
        low_arr = np.asarray(lower_price, dtype=float)
        high_arr = np.asarray(upper_price, dtype=float)
        metrics["coverage_95"] = float(np.mean((y_true >= low_arr) & (y_true <= high_arr)))

    return metrics


class DirectMultiHorizonFXModel(_PythonModelBase):  # type: ignore[misc]
    """MLflow PyFunc wrapper holding direct LightGBM point + calibrated quantile models for h=1..7."""

    def __init__(
        self,
        horizon_artifacts: Mapping[int, HorizonArtifact],
        feature_columns: Sequence[str] = FEATURE_COLUMNS,
    ) -> None:
        self.horizon_artifacts = dict(sorted(horizon_artifacts.items()))
        self.feature_columns = tuple(feature_columns)

    def predict(self, context: Any, model_input: pd.DataFrame) -> pd.DataFrame:  # noqa: ARG002
        """Predict log-returns and monotonic calibrated 95% bounds for all horizons."""
        missing = [c for c in self.feature_columns if c not in model_input.columns]
        if missing:
            raise ValueError(f"Missing required feature columns: {missing}")

        x_mat = model_input.loc[:, list(self.feature_columns)].astype(float)
        records: list[dict[str, float | int]] = []
        for horizon, artifact in self.horizon_artifacts.items():
            pred_r = np.asarray(artifact.point_model.predict(x_mat), dtype=float)
            raw_low = np.asarray(artifact.lower_model.predict(x_mat), dtype=float) - artifact.conformal_adjustment
            raw_high = np.asarray(artifact.upper_model.predict(x_mat), dtype=float) + artifact.conformal_adjustment

            # Enforce bound monotonicity around point prediction
            low_r = np.minimum(raw_low, pred_r)
            high_r = np.maximum(raw_high, pred_r)

            for row_idx in range(len(x_mat)):
                records.append(
                    {
                        "row_index": row_idx,
                        "horizon_days": int(horizon),
                        "pred_log_return": float(pred_r[row_idx]),
                        "lower_log_return": float(low_r[row_idx]),
                        "upper_log_return": float(high_r[row_idx]),
                    }
                )

        return pd.DataFrame(records)


def _default_lgbm_factory(**kwargs: Any) -> Any:
    import lightgbm as lgb

    return lgb.LGBMRegressor(**kwargs)


def _fit_single_estimator(
    estimator: Any,
    x_train: pd.DataFrame,
    y_train: np.ndarray,
    x_val: pd.DataFrame,
    y_val: np.ndarray,
    early_stopping_rounds: int = 30,
) -> Any:
    try:
        import lightgbm as lgb

        if isinstance(estimator, lgb.LGBMRegressor):
            estimator.fit(
                x_train,
                y_train,
                eval_set=[(x_val, y_val)],
                callbacks=[lgb.early_stopping(stopping_rounds=early_stopping_rounds, verbose=False)],
            )
            return estimator
    except ImportError:
        pass

    estimator.fit(x_train, y_train)
    return estimator


def fit_horizon_artifact(
    horizon: int,
    x_train: pd.DataFrame,
    y_train: np.ndarray,
    x_val: pd.DataFrame,
    y_val: np.ndarray,
    params: Optional[Mapping[str, Any]] = None,
    estimator_factory: Optional[Callable[..., Any]] = None,
    alpha: float = 0.05,
) -> HorizonArtifact:
    """Fit point and 2.5%/97.5% quantile models for horizon ``h`` using ``val`` only for early stopping."""
    factory = estimator_factory or _default_lgbm_factory
    base_params = dict(DEFAULT_LGBM_PARAMS)
    if params:
        base_params.update(params)

    point_params = {**base_params, "objective": "regression", "metric": "rmse"}
    lower_params = {**base_params, "objective": "quantile", "metric": "quantile", "alpha": alpha / 2.0}
    upper_params = {**base_params, "objective": "quantile", "metric": "quantile", "alpha": 1.0 - alpha / 2.0}

    point_model = _fit_single_estimator(factory(**point_params), x_train, y_train, x_val, y_val)
    lower_model = _fit_single_estimator(factory(**lower_params), x_train, y_train, x_val, y_val)
    upper_model = _fit_single_estimator(factory(**upper_params), x_train, y_train, x_val, y_val)

    q_low_val = np.asarray(lower_model.predict(x_val), dtype=float)
    q_high_val = np.asarray(upper_model.predict(x_val), dtype=float)
    cqr_adj = compute_conformal_quantile_adjustment(y_val, q_low_val, q_high_val, alpha=alpha)

    return HorizonArtifact(
        horizon=horizon,
        point_model=point_model,
        lower_model=lower_model,
        upper_model=upper_model,
        conformal_adjustment=cqr_adj,
    )


def evaluate_direct_bundle(
    df_data: pd.DataFrame,
    split: TemporalSplit,
    feature_columns: Sequence[str] = FEATURE_COLUMNS,
    horizons: Sequence[int] = TARGET_HORIZONS,
    params: Optional[Mapping[str, Any]] = None,
    estimator_factory: Optional[Callable[..., Any]] = None,
) -> tuple[DirectMultiHorizonFXModel, dict[str, float]]:
    """Train direct h=1..7 models on ``split.train/val`` and evaluate strictly on ``split.test``."""
    x_all = df_data.loc[:, list(feature_columns)].astype(float)
    spot_test = df_data["close_price"].iloc[split.test].to_numpy(dtype=float)

    artifacts: dict[int, HorizonArtifact] = {}
    metrics: dict[str, float] = {}
    rmses: list[float] = []
    maes: list[float] = []
    mapes: list[float] = []
    naive_rmses: list[float] = []
    dir_accs: list[float] = []
    coverages: list[float] = []

    x_train = x_all.iloc[split.train]
    x_val = x_all.iloc[split.val]
    x_test = x_all.iloc[split.test]

    for h in horizons:
        target_col = f"target_log_return_h{h}"
        y_all = df_data[target_col].to_numpy(dtype=float)
        artifact = fit_horizon_artifact(
            horizon=h,
            x_train=x_train,
            y_train=y_all[split.train],
            x_val=x_val,
            y_val=y_all[split.val],
            params=params,
            estimator_factory=estimator_factory,
        )
        artifacts[h] = artifact

        # True test closing price P_{t+h} = P_t * exp(y_{t,h})
        y_true_price = np.asarray(reconstruct_price(spot_test, y_all[split.test]), dtype=float)
        pred_r = np.asarray(artifact.point_model.predict(x_test), dtype=float)
        low_r = np.minimum(
            np.asarray(artifact.lower_model.predict(x_test), dtype=float) - artifact.conformal_adjustment,
            pred_r,
        )
        high_r = np.maximum(
            np.asarray(artifact.upper_model.predict(x_test), dtype=float) + artifact.conformal_adjustment,
            pred_r,
        )

        y_pred_price = np.asarray(reconstruct_price(spot_test, pred_r), dtype=float)
        lower_price = np.asarray(reconstruct_price(spot_test, low_r), dtype=float)
        upper_price = np.asarray(reconstruct_price(spot_test, high_r), dtype=float)

        h_metrics = compute_horizon_metrics(
            y_true_price=y_true_price,
            y_pred_price=y_pred_price,
            base_price=spot_test,
            lower_price=lower_price,
            upper_price=upper_price,
        )
        for k, v in h_metrics.items():
            metrics[f"h{h}_{k}"] = v

        rmses.append(h_metrics["rmse"])
        maes.append(h_metrics["mae"])
        mapes.append(h_metrics["mape"])
        naive_rmses.append(h_metrics["naive_rmse"])
        dir_accs.append(h_metrics["directional_accuracy"])
        coverages.append(h_metrics["coverage_95"])

    mean_rmse = float(np.mean(rmses))
    mean_naive_rmse = float(np.mean(naive_rmses))
    metrics["mean_rmse"] = mean_rmse
    metrics["mean_mae"] = float(np.mean(maes))
    metrics["mean_mape"] = float(np.mean(mapes))
    metrics["mean_naive_rmse"] = mean_naive_rmse
    metrics["mean_skill_vs_naive"] = float(1.0 - (mean_rmse / mean_naive_rmse)) if mean_naive_rmse > 0.0 else 0.0
    metrics["mean_directional_accuracy"] = float(np.mean(dir_accs))
    metrics["mean_coverage_95"] = float(np.mean(coverages))

    bundle = DirectMultiHorizonFXModel(horizon_artifacts=artifacts, feature_columns=feature_columns)
    return bundle, metrics


def run_walk_forward_cv(
    df_data: pd.DataFrame,
    n_splits: int = 4,
    val_size: int = 20,
    test_size: int = 20,
    min_train_size: int = 120,
    feature_columns: Sequence[str] = FEATURE_COLUMNS,
    horizons: Sequence[int] = TARGET_HORIZONS,
    params: Optional[Mapping[str, Any]] = None,
    estimator_factory: Optional[Callable[..., Any]] = None,
) -> dict[str, float]:
    """Run expanding-window walk-forward CV across ``n_splits`` historical folds."""
    folds = generate_walk_forward_folds(
        n_samples=len(df_data),
        n_splits=n_splits,
        val_size=val_size,
        test_size=test_size,
        min_train_size=min_train_size,
    )
    fold_metrics: list[dict[str, float]] = []
    for fold in folds:
        _, m = evaluate_direct_bundle(
            df_data=df_data,
            split=fold,
            feature_columns=feature_columns,
            horizons=horizons,
            params=params,
            estimator_factory=estimator_factory,
        )
        fold_metrics.append(m)

    cv_mean_rmse = float(np.mean([m["mean_rmse"] for m in fold_metrics]))
    cv_mean_naive_rmse = float(np.mean([m["mean_naive_rmse"] for m in fold_metrics]))
    cv_mean_dir_acc = float(np.mean([m["mean_directional_accuracy"] for m in fold_metrics]))
    cv_mean_cov = float(np.mean([m["mean_coverage_95"] for m in fold_metrics]))

    return {
        "cv_mean_rmse": cv_mean_rmse,
        "cv_mean_naive_rmse": cv_mean_naive_rmse,
        "cv_skill_vs_naive": float(1.0 - (cv_mean_rmse / cv_mean_naive_rmse)) if cv_mean_naive_rmse > 0.0 else 0.0,
        "cv_mean_directional_accuracy": cv_mean_dir_acc,
        "cv_mean_coverage_95": cv_mean_cov,
    }


def should_promote_challenger(
    challenger_metrics: Mapping[str, float],
    champion_metrics: Optional[Mapping[str, float]] = None,
    metric_key: str = "mean_rmse",
    naive_key: str = "mean_naive_rmse",
) -> tuple[bool, str]:
    """Decide whether to promote ``@challenger`` to ``@champion``.

    Promotion requires:
    1. Finite challenger RMSE strictly beating the naive random-walk baseline.
    2. If an existing ``@champion`` exists, strictly beating its holdout/CV RMSE.
    """
    challenger_val = challenger_metrics.get(metric_key)
    naive_val = challenger_metrics.get(naive_key)
    if challenger_val is None or naive_val is None or not (math.isfinite(challenger_val) and math.isfinite(naive_val)):
        return False, "Challenger metrics missing or non-finite."

    if challenger_val >= naive_val:
        return (
            False,
            f"Challenger {metric_key}={challenger_val:.6f} failed to beat naive baseline {naive_key}={naive_val:.6f}.",
        )

    if champion_metrics is not None and metric_key in champion_metrics:
        champion_val = champion_metrics[metric_key]
        if math.isfinite(champion_val) and challenger_val >= champion_val:
            return (
                False,
                f"Challenger {metric_key}={challenger_val:.6f} did not beat existing champion ({champion_val:.6f}).",
            )

    return True, f"Challenger {metric_key}={challenger_val:.6f} beat naive baseline and current champion."
