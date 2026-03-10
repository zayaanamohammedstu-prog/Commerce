"""
models/forecasting.py
Time-series sales forecasting using Exponential Smoothing (Holt-Winters).
Falls back to a simple moving-average trend if statsmodels is unavailable.
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import List, Dict

import numpy as np
import pandas as pd

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    _HAS_STATSMODELS = True
except ImportError:
    _HAS_STATSMODELS = False

from warehouse.database import get_connection, DB_PATH


def _load_daily_sales(db_path: str = DB_PATH) -> pd.Series:
    """Query the warehouse for an ordered daily revenue time series."""
    conn = get_connection(db_path)
    query = """
        SELECT date_id, SUM(total_amount) AS revenue
        FROM fact_sales
        GROUP BY date_id
        ORDER BY date_id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return pd.Series(dtype=float)
    df["date_id"] = pd.to_datetime(df["date_id"])
    df.set_index("date_id", inplace=True)
    # Reindex to fill any missing calendar days with 0
    full_idx = pd.date_range(df.index.min(), df.index.max(), freq="D")
    series = df["revenue"].reindex(full_idx, fill_value=0.0)
    return series


def _moving_average_forecast(series: pd.Series, horizon: int) -> List[float]:
    """Simple rolling-mean forecast used as a fallback."""
    window = min(30, len(series))
    level = float(series.iloc[-window:].mean())
    return [round(level, 2)] * horizon


def forecast_sales(horizon: int = 30, db_path: str = DB_PATH) -> List[Dict]:
    """
    Forecast daily revenue for the next `horizon` days.
    Returns a list of dicts: [{"date": "YYYY-MM-DD", "forecast": float}, ...]
    """
    series = _load_daily_sales(db_path)
    if series.empty:
        last_date = date.today()
        return [
            {"date": (last_date + timedelta(days=i + 1)).isoformat(), "forecast": 0.0}
            for i in range(horizon)
        ]

    last_date = series.index[-1].date()

    if _HAS_STATSMODELS and len(series) >= 14:
        try:
            model = ExponentialSmoothing(
                series,
                trend="add",
                seasonal="add",
                seasonal_periods=7,
                initialization_method="estimated",
            )
            fit = model.fit(optimized=True, disp=False)
            raw = fit.forecast(horizon)
            values = [max(0.0, round(v, 2)) for v in raw]
        except Exception:
            values = _moving_average_forecast(series, horizon)
    else:
        values = _moving_average_forecast(series, horizon)

    results = []
    for i, val in enumerate(values):
        forecast_date = last_date + timedelta(days=i + 1)
        results.append({"date": forecast_date.isoformat(), "forecast": val})
    return results


def forecast_from_dataframe(df: pd.DataFrame, horizon: int = 30) -> Dict:
    """
    Forecast from an uploaded DataFrame.

    Expected columns (case-insensitive):
    - ``date``    – date strings parseable by pandas
    - ``revenue`` – numeric daily revenue  (or ``total_amount`` as alias)
      OR both ``quantity`` and ``unit_price`` to compute revenue on the fly.

    Returns a dict with keys: forecast, horizon, training_days,
    training_start, training_end, mean_daily_revenue.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # Resolve revenue column
    if "revenue" not in df.columns:
        if "total_amount" in df.columns:
            df["revenue"] = df["total_amount"]
        elif "quantity" in df.columns and "unit_price" in df.columns:
            df["revenue"] = (
                pd.to_numeric(df["quantity"], errors="coerce")
                * pd.to_numeric(df["unit_price"], errors="coerce")
            )
        else:
            raise ValueError(
                "CSV must contain a 'revenue' column, a 'total_amount' column, "
                "or both 'quantity' and 'unit_price' columns."
            )

    if "date" not in df.columns:
        raise ValueError("CSV must contain a 'date' column.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce").fillna(0.0)

    # Aggregate to daily totals
    series = df.groupby("date")["revenue"].sum().sort_index()
    if series.empty:
        raise ValueError("No valid data rows found in the uploaded CSV.")

    # Fill any missing calendar days
    full_idx = pd.date_range(series.index.min(), series.index.max(), freq="D")
    series = series.reindex(full_idx, fill_value=0.0)

    last_date = series.index[-1].date()
    # Weekly seasonality needs at least 2 complete periods (14 days)
    _MIN_DAYS_FOR_SEASONAL = 14
    use_seasonal = len(series) >= _MIN_DAYS_FOR_SEASONAL

    if _HAS_STATSMODELS and len(series) >= 2:
        try:
            model = ExponentialSmoothing(
                series,
                trend="add",
                seasonal="add" if use_seasonal else None,
                seasonal_periods=7 if use_seasonal else None,
                initialization_method="estimated",
            )
            fit = model.fit(optimized=True, disp=False)
            raw = fit.forecast(horizon)
            values = [max(0.0, round(v, 2)) for v in raw]
        except Exception:
            values = _moving_average_forecast(series, horizon)
    else:
        values = _moving_average_forecast(series, horizon)

    forecast_results = [
        {"date": (last_date + timedelta(days=i + 1)).isoformat(), "forecast": val}
        for i, val in enumerate(values)
    ]

    return {
        "horizon": horizon,
        "training_days": len(series),
        "training_start": series.index[0].date().isoformat(),
        "training_end": last_date.isoformat(),
        "mean_daily_revenue": round(float(series.mean()), 2),
        "forecast": forecast_results,
    }


def get_model_summary(db_path: str = DB_PATH) -> Dict:
    """Return high-level metadata about the forecasting model and data."""
    series = _load_daily_sales(db_path)
    if series.empty:
        return {"status": "no_data"}
    return {
        "status": "ready",
        "algorithm": "Holt-Winters Exponential Smoothing" if _HAS_STATSMODELS else "Moving Average",
        "training_start": series.index[0].date().isoformat(),
        "training_end": series.index[-1].date().isoformat(),
        "training_days": len(series),
        "mean_daily_revenue": round(float(series.mean()), 2),
        "std_daily_revenue": round(float(series.std()), 2),
    }
