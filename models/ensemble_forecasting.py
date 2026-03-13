"""
models/ensemble_forecasting.py
Multi-horizon ensemble forecasting for ShopIntel.

Combines three model families:
  - Short-term  (sklearn): lag-feature regression with holiday flags
  - Medium-term (Prophet, optional): trend + weekly/yearly seasonality
  - Long-term   (statsmodels Holt-Winters): smooth trend/seasonality

Dynamic weights are computed from recent holdout accuracy (lower error → higher weight).
Returns per-day forecasts with prediction intervals (empirical bootstrap).
"""
from __future__ import annotations

import logging
import warnings
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import holidays as _holidays_pkg
    _HAS_HOLIDAYS = True
except ImportError:
    _HAS_HOLIDAYS = False

try:
    from prophet import Prophet as _Prophet
    _HAS_PROPHET = True
except ImportError:
    _HAS_PROPHET = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

try:
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False

from warehouse.database import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_MIN_ROWS_SHORTTERM = 14    # minimum days for short-term model
_MIN_ROWS_PROPHET   = 14
_MIN_ROWS_HOLTWINT  = 14
_BOOTSTRAP_ITERS    = 500
_CONFIDENCE_LEVEL   = 0.80  # 80 % prediction interval


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_daily_sales(db_path: str) -> pd.DataFrame:
    """Load daily revenue from the warehouse, filling missing dates with 0."""
    conn = get_connection(db_path)
    query = (
        "SELECT date_id AS ds, SUM(total_amount) AS y "
        "FROM fact_sales GROUP BY date_id ORDER BY date_id"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])
    full_idx = pd.date_range(df["ds"].min(), df["ds"].max(), freq="D")
    df = df.set_index("ds").reindex(full_idx, fill_value=0.0).reset_index()
    df.columns = ["ds", "y"]
    return df


# ---------------------------------------------------------------------------
# Holiday calendar helper
# ---------------------------------------------------------------------------

def _get_holiday_set(start: date, end: date, country: str = "US") -> set:
    """Return a set of holiday dates for the given range."""
    if not _HAS_HOLIDAYS:
        return set()
    try:
        cal = _holidays_pkg.country_holidays(country)
        result = set()
        d = start
        while d <= end:
            if d in cal:
                result.add(d)
            d += timedelta(days=1)
        return result
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Feature engineering for short-term model
# ---------------------------------------------------------------------------

def _build_features(df: pd.DataFrame, holiday_set: set) -> pd.DataFrame:
    """Create lag/calendar features for the short-term regression."""
    out = df.copy()
    out["dayofweek"]  = out["ds"].dt.dayofweek
    out["month"]      = out["ds"].dt.month
    out["dayofmonth"] = out["ds"].dt.day
    out["weekofyear"] = out["ds"].dt.isocalendar().week.astype(int)
    out["is_weekend"] = (out["ds"].dt.dayofweek >= 5).astype(int)
    out["is_holiday"] = out["ds"].apply(lambda d: int(d.date() in holiday_set))

    for lag in [1, 2, 3, 7, 14]:
        out[f"lag_{lag}"] = out["y"].shift(lag)
    out["rolling_7"]  = out["y"].shift(1).rolling(7,  min_periods=1).mean()
    out["rolling_14"] = out["y"].shift(1).rolling(14, min_periods=1).mean()
    out["rolling_30"] = out["y"].shift(1).rolling(30, min_periods=1).mean()
    return out


_FEATURE_COLS = [
    "dayofweek", "month", "dayofmonth", "weekofyear",
    "is_weekend", "is_holiday",
    "lag_1", "lag_2", "lag_3", "lag_7", "lag_14",
    "rolling_7", "rolling_14", "rolling_30",
]


# ---------------------------------------------------------------------------
# Individual model trainers
# ---------------------------------------------------------------------------

def _shortterm_model(
    df: pd.DataFrame,
    holiday_set: set,
    horizon: int,
) -> Tuple[List[float], List[float], List[float], float]:
    """
    Gradient-boosting short-term forecast.
    Returns (yhat, lower, upper, test_mae).
    """
    if not _HAS_SKLEARN or len(df) < _MIN_ROWS_SHORTTERM:
        return [], [], [], float("inf")

    feat_df = _build_features(df, holiday_set).dropna(subset=_FEATURE_COLS)
    if len(feat_df) < 7:
        return [], [], [], float("inf")

    test_size = max(7, int(len(feat_df) * 0.2))
    train = feat_df.iloc[:-test_size]
    test  = feat_df.iloc[-test_size:]

    X_train = train[_FEATURE_COLS].values
    y_train = train["y"].values
    X_test  = test[_FEATURE_COLS].values
    y_test  = test["y"].values

    model = GradientBoostingRegressor(n_estimators=200, max_depth=4, random_state=42)
    model.fit(X_train, y_train)
    test_pred = model.predict(X_test)
    test_mae  = float(np.mean(np.abs(y_test - test_pred)))

    # Retrain on full data
    X_all = feat_df[_FEATURE_COLS].values
    y_all = feat_df["y"].values
    model.fit(X_all, y_all)

    # Recursive multi-step forecast
    last_date  = df["ds"].iloc[-1]
    y_history  = list(df["y"].values)
    ds_history = list(df["ds"].values)

    preds = []
    for step in range(horizon):
        next_ds = last_date + timedelta(days=step + 1)
        ds_history.append(next_ds)
        y_history.append(0.0)   # placeholder; replaced below with the predicted value
        tmp = pd.DataFrame({"ds": ds_history, "y": y_history})
        feat_row = _build_features(tmp, holiday_set).iloc[-1][_FEATURE_COLS].values
        val = float(model.predict(feat_row.reshape(1, -1))[0])
        preds.append(max(0.0, round(val, 2)))
        # Replace placeholder with predicted value for lag computations in subsequent steps
        y_history[-1] = val

    # Empirical residual intervals
    train_pred = model.predict(X_all)
    residuals  = y_all - train_pred
    q_lo = float(np.percentile(residuals, (1 - _CONFIDENCE_LEVEL) / 2 * 100))
    q_hi = float(np.percentile(residuals, (1 - (1 - _CONFIDENCE_LEVEL) / 2) * 100))

    lower = [max(0.0, round(v + q_lo, 2)) for v in preds]
    upper = [round(v + q_hi, 2) for v in preds]

    return preds, lower, upper, test_mae


def _prophet_model(
    df: pd.DataFrame,
    holiday_set: set,
    horizon: int,
    country: str = "US",
) -> Tuple[List[float], List[float], List[float], float]:
    """
    Prophet medium-term forecast.
    Returns (yhat, lower, upper, test_mae).
    """
    if not _HAS_PROPHET or len(df) < _MIN_ROWS_PROPHET:
        return [], [], [], float("inf")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            n         = len(df)
            test_size = max(7, int(n * 0.2))
            train_df  = df.iloc[:-test_size].copy()
            test_df   = df.iloc[-test_size:].copy()

            def _make_prophet(n_rows):
                return _Prophet(
                    daily_seasonality=False,
                    weekly_seasonality=True,
                    yearly_seasonality=(n_rows >= 365),
                    interval_width=_CONFIDENCE_LEVEL,
                )

            m = _make_prophet(len(train_df))
            if holiday_set:
                holiday_dates = [str(d) for d in sorted(holiday_set)]
                h_df = pd.DataFrame({
                    "holiday": "public_holiday",
                    "ds": pd.to_datetime(holiday_dates),
                })
                m.add_country_holidays(country_name=country)
            m.fit(train_df)

            ftest  = m.make_future_dataframe(periods=test_size)
            pred_t = m.predict(ftest)
            test_mae = float(
                np.mean(np.abs(
                    test_df["y"].values - pred_t.iloc[-test_size:]["yhat"].values
                ))
            )

            full_m = _make_prophet(n)
            if holiday_set:
                full_m.add_country_holidays(country_name=country)
            full_m.fit(df)

            future   = full_m.make_future_dataframe(periods=horizon)
            forecast = full_m.predict(future)
            tail     = forecast.iloc[-horizon:]

            yhat  = [max(0.0, round(v, 2)) for v in tail["yhat"].values]
            lower = [max(0.0, round(v, 2)) for v in tail["yhat_lower"].values]
            upper = [round(v, 2) for v in tail["yhat_upper"].values]

            return yhat, lower, upper, test_mae
        except Exception as exc:
            logger.warning("Prophet model failed: %s", exc)
            return [], [], [], float("inf")


def _holtwinters_model(
    df: pd.DataFrame,
    horizon: int,
) -> Tuple[List[float], List[float], List[float], float]:
    """
    Holt-Winters long-term forecast with empirical intervals.
    Returns (yhat, lower, upper, test_mae).
    """
    if not _HAS_STATSMODELS or len(df) < _MIN_ROWS_HOLTWINT:
        return [], [], [], float("inf")

    try:
        n         = len(df)
        test_size = max(7, int(n * 0.2))
        train_y   = df["y"].iloc[:-test_size]
        test_y    = df["y"].iloc[-test_size:]

        use_seasonal    = len(train_y) >= 14
        seasonal_periods = 7 if use_seasonal else None

        def _fit(series):
            m = ExponentialSmoothing(
                series,
                trend="add",
                seasonal="add" if use_seasonal else None,
                seasonal_periods=seasonal_periods,
                initialization_method="estimated",
            )
            return m.fit(optimized=True, disp=False)

        test_fit  = _fit(train_y)
        test_pred = test_fit.forecast(test_size)
        test_mae  = float(np.mean(np.abs(test_y.values - test_pred.values)))

        full_fit  = _fit(df["y"])
        preds     = full_fit.forecast(horizon)
        yhat      = [max(0.0, round(v, 2)) for v in preds]

        # Bootstrap residual intervals
        residuals = df["y"].values - full_fit.fittedvalues.values
        rng = np.random.default_rng(42)
        sims = np.array([
            np.array(yhat) + rng.choice(residuals, size=horizon, replace=True)
            for _ in range(_BOOTSTRAP_ITERS)
        ])
        alpha = (1 - _CONFIDENCE_LEVEL) / 2
        lower = [max(0.0, round(v, 2)) for v in np.percentile(sims, alpha * 100, axis=0)]
        upper = [round(v, 2) for v in np.percentile(sims, (1 - alpha) * 100, axis=0)]

        return yhat, lower, upper, test_mae
    except Exception as exc:
        logger.warning("Holt-Winters model failed: %s", exc)
        return [], [], [], float("inf")


# ---------------------------------------------------------------------------
# Weight computation
# ---------------------------------------------------------------------------

def _compute_weights(maes: Dict[str, float]) -> Dict[str, float]:
    """
    Convert per-model MAE values into normalised weights (lower MAE → higher weight).
    Models that failed (MAE=inf) receive weight 0.
    """
    finite = {k: v for k, v in maes.items() if np.isfinite(v) and v < 1e12}
    if not finite:
        available = list(maes.keys())
        return {k: 1.0 / len(available) for k in available}

    inv = {k: 1.0 / (v + 1e-8) for k, v in finite.items()}
    total = sum(inv.values())
    weights = {k: round(v / total, 4) for k, v in inv.items()}
    # Assign 0 to failed models
    for k in maes:
        if k not in weights:
            weights[k] = 0.0
    return weights


# ---------------------------------------------------------------------------
# Ensemble forecast (main entry point)
# ---------------------------------------------------------------------------

def ensemble_forecast(
    horizon: int = 30,
    db_path: str = None,
    country: str = "US",
) -> Dict:
    """
    Produce an ensemble forecast for the next `horizon` days.

    Returns a dict:
      {
        "forecast": [{"date": ..., "yhat": ..., "yhat_lower": ..., "yhat_upper": ...}],
        "algorithm": "Ensemble",
        "weights": {"short_term": 0.4, "prophet": 0.35, "holt_winters": 0.25},
        "mae": ..., "rmse": ..., "mape": ...,
        "training_start": ..., "training_end": ..., "training_days": ...,
        "errors": [],          # list of non-fatal error messages
      }
    """
    errors: List[str] = []

    df = _load_daily_sales(db_path)
    if df.empty:
        today = date.today()
        fallback = [
            {
                "date": (today + timedelta(days=i + 1)).isoformat(),
                "yhat": 0.0,
                "yhat_lower": 0.0,
                "yhat_upper": 0.0,
            }
            for i in range(horizon)
        ]
        return {
            "forecast": fallback,
            "algorithm": "Ensemble",
            "weights": {},
            "mae": None, "rmse": None, "mape": None,
            "training_start": None, "training_end": None, "training_days": 0,
            "errors": ["No sales data available."],
        }

    last_date = df["ds"].iloc[-1].date()
    dates = [(last_date + timedelta(days=i + 1)) for i in range(horizon)]

    start_d = df["ds"].iloc[0].date()
    end_d   = last_date + timedelta(days=horizon)
    holiday_set = _get_holiday_set(start_d, end_d, country)

    # --- Run all three models ---
    st_yhat, st_lo, st_hi, st_mae = _shortterm_model(df, holiday_set, horizon)
    if not st_yhat:
        errors.append("Short-term model unavailable (insufficient data or sklearn missing).")

    pr_yhat, pr_lo, pr_hi, pr_mae = _prophet_model(df, holiday_set, horizon, country)
    if not pr_yhat:
        errors.append("Prophet model unavailable (insufficient data or not installed).")

    hw_yhat, hw_lo, hw_hi, hw_mae = _holtwinters_model(df, horizon)
    if not hw_yhat:
        errors.append("Holt-Winters model unavailable (insufficient data or statsmodels missing).")

    # --- Weights ---
    maes = {
        "short_term":   st_mae if st_yhat else float("inf"),
        "prophet":      pr_mae if pr_yhat else float("inf"),
        "holt_winters": hw_mae if hw_yhat else float("inf"),
    }
    weights = _compute_weights(maes)

    # --- Ensemble blend ---
    def _blend(arrays: List[Tuple[List[float], str]]) -> List[float]:
        result = np.zeros(horizon)
        for arr, name in arrays:
            if arr:
                result += weights[name] * np.array(arr[:horizon])
        return [round(float(v), 2) for v in result]

    yhat_arrs  = [(st_yhat, "short_term"), (pr_yhat, "prophet"), (hw_yhat, "holt_winters")]
    lower_arrs = [(st_lo,   "short_term"), (pr_lo,   "prophet"), (hw_lo,   "holt_winters")]
    upper_arrs = [(st_hi,   "short_term"), (pr_hi,   "prophet"), (hw_hi,   "holt_winters")]

    yhat  = _blend(yhat_arrs)
    lower = _blend(lower_arrs)
    upper = _blend(upper_arrs)

    # If no model produced a result, fall back to moving average
    if all(v == 0.0 for v in yhat):
        window = min(30, len(df))
        level  = float(df["y"].iloc[-window:].mean())
        yhat   = [round(max(0.0, level), 2)] * horizon
        lower  = [round(max(0.0, level * 0.85), 2)] * horizon
        upper  = [round(level * 1.15, 2)] * horizon
        errors.append("Fell back to moving average (all models failed).")

    forecast_list = [
        {
            "date":       d.isoformat(),
            "yhat":       yhat[i],
            "yhat_lower": lower[i],
            "yhat_upper": upper[i],
        }
        for i, d in enumerate(dates)
    ]

    # --- Ensemble accuracy (weighted average MAE of component models) ---
    finite_maes = [v for v in maes.values() if np.isfinite(v)]
    ensemble_mae  = round(float(np.mean(finite_maes)), 2) if finite_maes else None
    ensemble_rmse = round(float(np.sqrt(np.mean([v ** 2 for v in finite_maes]))), 2) if finite_maes else None

    # MAPE computed using Holt-Winters model on holdout data
    mape = None
    if _HAS_STATSMODELS and len(df) >= 14:
        try:
            test_size = max(7, int(len(df) * 0.2))
            train_y   = df["y"].iloc[:-test_size]
            test_y    = df["y"].iloc[-test_size:]
            m = ExponentialSmoothing(
                train_y,
                trend="add",
                seasonal="add" if len(train_y) >= 14 else None,
                seasonal_periods=7 if len(train_y) >= 14 else None,
                initialization_method="estimated",
            )
            fit  = m.fit(optimized=True, disp=False)
            pred = fit.forecast(test_size).values
            mape = round(
                float(np.mean(np.abs((test_y.values - pred) / (test_y.values + 1e-8))) * 100),
                2,
            )
        except Exception:
            pass

    return {
        "forecast":       forecast_list,
        "algorithm":      "Ensemble",
        "weights":        weights,
        "mae":            ensemble_mae,
        "rmse":           ensemble_rmse,
        "mape":           mape,
        "training_start": df["ds"].iloc[0].date().isoformat(),
        "training_end":   last_date.isoformat(),
        "training_days":  len(df),
        "errors":         errors,
    }
