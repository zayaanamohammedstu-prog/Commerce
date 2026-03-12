"""
models/prophet_forecasting.py
Prophet-based time-series forecasting with accuracy metrics.
Falls back to Holt-Winters/moving-average if prophet is unavailable.
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import List, Dict

import numpy as np
import pandas as pd

try:
    from prophet import Prophet
    _HAS_PROPHET = True
except ImportError:
    _HAS_PROPHET = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

from warehouse.database import get_connection


def _load_daily_sales(db_path: str) -> pd.DataFrame:
    conn = get_connection(db_path)
    query = "SELECT date_id AS ds, SUM(total_amount) AS y FROM fact_sales GROUP BY date_id ORDER BY date_id"
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return pd.DataFrame(columns=["ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])
    full_idx = pd.date_range(df["ds"].min(), df["ds"].max(), freq="D")
    df = df.set_index("ds").reindex(full_idx, fill_value=0.0).reset_index()
    df.columns = ["ds", "y"]
    return df


def _moving_average_values(series: pd.Series, horizon: int) -> List[float]:
    window = min(30, len(series))
    level = float(series.iloc[-window:].mean())
    return [round(max(0.0, level), 2)] * horizon


def _prophet_forecast(df: pd.DataFrame, horizon: int) -> tuple[List[float], dict]:
    """Run Prophet and return (values, accuracy_metrics)."""
    if len(df) < 14:
        raise ValueError("Not enough data for Prophet (need >= 14 days)")

    # Hold out last 20% for accuracy
    n = len(df)
    test_size = max(7, int(n * 0.2))
    train_df = df.iloc[:-test_size].copy()
    test_df = df.iloc[-test_size:].copy()

    model = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=(n >= 365))
    model.fit(train_df)

    # Accuracy on holdout
    future_test = model.make_future_dataframe(periods=test_size)
    forecast_test = model.predict(future_test)
    test_pred = forecast_test.iloc[-test_size:]["yhat"].values
    test_actual = test_df["y"].values

    mae = float(np.mean(np.abs(test_actual - test_pred)))
    rmse = float(np.sqrt(np.mean((test_actual - test_pred) ** 2)))
    mean_actual = float(np.mean(np.abs(test_actual)))
    mape = float(np.mean(np.abs((test_actual - test_pred) / (test_actual + 1e-8))) * 100)

    # Full model for forecast
    full_model = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=(n >= 365))
    full_model.fit(df)
    future = full_model.make_future_dataframe(periods=horizon)
    forecast = full_model.predict(future)
    values = [max(0.0, round(v, 2)) for v in forecast.iloc[-horizon:]["yhat"].values]

    accuracy = {"mae": round(mae, 2), "rmse": round(rmse, 2), "mape": round(mape, 2), "test_days": test_size}
    return values, accuracy


def forecast_sales_prophet(horizon: int = 30, db_path: str = None) -> List[Dict]:
    """Forecast daily revenue for the next horizon days."""
    df = _load_daily_sales(db_path)
    if df.empty:
        last_date = date.today()
        return [{"date": (last_date + timedelta(days=i+1)).isoformat(), "forecast": 0.0} for i in range(horizon)]

    last_date = df["ds"].iloc[-1].date()

    if _HAS_PROPHET and len(df) >= 14:
        try:
            values, _ = _prophet_forecast(df, horizon)
        except Exception:
            values = _moving_average_values(df["y"], horizon)
    elif _HAS_STATSMODELS and len(df) >= 14:
        try:
            from statsmodels.tsa.holtwinters import ExponentialSmoothing
            use_seasonal = len(df) >= 14
            model = ExponentialSmoothing(df["y"], trend="add",
                                         seasonal="add" if use_seasonal else None,
                                         seasonal_periods=7 if use_seasonal else None,
                                         initialization_method="estimated")
            fit = model.fit(optimized=True, disp=False)
            raw = fit.forecast(horizon)
            values = [max(0.0, round(v, 2)) for v in raw]
        except Exception:
            values = _moving_average_values(df["y"], horizon)
    else:
        values = _moving_average_values(df["y"], horizon)

    return [{"date": (last_date + timedelta(days=i+1)).isoformat(), "forecast": val}
            for i, val in enumerate(values)]


def forecast_accuracy(db_path: str = None) -> Dict:
    """Run holdout validation and return accuracy metrics."""
    df = _load_daily_sales(db_path)
    if df.empty or len(df) < 20:
        return {"status": "insufficient_data", "message": "Need at least 20 days of data for accuracy metrics"}

    if _HAS_PROPHET and len(df) >= 14:
        try:
            _, accuracy = _prophet_forecast(df, 7)
            accuracy["algorithm"] = "Prophet"
            accuracy["status"] = "ok"
            return accuracy
        except Exception:
            pass

    # Fallback with Holt-Winters
    test_size = max(7, int(len(df) * 0.2))
    train = df["y"].iloc[:-test_size]
    test = df["y"].iloc[-test_size:]
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        model = ExponentialSmoothing(train, trend="add", seasonal="add", seasonal_periods=7, initialization_method="estimated")
        fit = model.fit(optimized=True, disp=False)
        preds = fit.forecast(test_size)
        mae = float(np.mean(np.abs(test.values - preds.values)))
        rmse = float(np.sqrt(np.mean((test.values - preds.values) ** 2)))
        mape = float(np.mean(np.abs((test.values - preds.values) / (test.values + 1e-8))) * 100)
        return {"status": "ok", "algorithm": "Holt-Winters", "mae": round(mae, 2), "rmse": round(rmse, 2), "mape": round(mape, 2), "test_days": test_size}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_forecast_summary(db_path: str = None) -> Dict:
    """Return metadata about the forecasting model and data."""
    df = _load_daily_sales(db_path)
    if df.empty:
        return {"status": "no_data"}
    algorithm = "Prophet" if _HAS_PROPHET else ("Holt-Winters" if _HAS_STATSMODELS else "Moving Average")
    return {
        "status": "ready",
        "algorithm": algorithm,
        "training_start": df["ds"].iloc[0].date().isoformat(),
        "training_end": df["ds"].iloc[-1].date().isoformat(),
        "training_days": len(df),
        "mean_daily_revenue": round(float(df["y"].mean()), 2),
        "std_daily_revenue": round(float(df["y"].std()), 2),
    }
