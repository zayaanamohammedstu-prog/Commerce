"""
models/anomaly.py
Anomaly detection for daily revenue time series.

Methods used:
  - Robust z-score (median + MAD)
  - IQR-based detection

Detected anomalies are stored in the ``forecast_events`` table and can be
retrieved later ("event memory") to annotate future forecasts.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from warehouse.database import get_connection

logger = logging.getLogger(__name__)

_ROBUST_Z_THRESHOLD = 3.0   # |z| above this → anomaly
_MIN_ROWS = 7               # minimum days needed for detection


# ---------------------------------------------------------------------------
# Core detection
# ---------------------------------------------------------------------------

def _robust_zscore(series: pd.Series) -> pd.Series:
    """Compute modified z-scores using median and MAD."""
    median = series.median()
    mad    = (series - median).abs().median()
    if mad == 0:
        mad = series.std() or 1.0
    return (series - median).abs() / (mad + 1e-8) * 0.6745


def detect_anomalies(series: pd.Series, threshold: float = _ROBUST_Z_THRESHOLD) -> pd.DataFrame:
    """
    Detect anomalous values in a daily revenue series.

    Parameters
    ----------
    series : pd.Series  – indexed by datetime, values = daily revenue
    threshold : float   – robust z-score threshold

    Returns
    -------
    pd.DataFrame with columns: date, value, zscore, severity, description
    """
    if len(series) < _MIN_ROWS:
        return pd.DataFrame(columns=["date", "value", "zscore", "severity", "description"])

    zscores = _robust_zscore(series)
    anomalies = zscores[zscores > threshold]

    rows = []
    for ts, z in anomalies.items():
        val = float(series.loc[ts])
        sev = min(1.0, round(float(z) / (threshold * 2), 4))
        median_val = float(series.median())
        direction  = "spike" if val > median_val else "drop"
        pct_diff   = abs(val - median_val) / (median_val + 1e-8) * 100
        desc = (
            f"Revenue {direction} of {pct_diff:.0f}% vs median "
            f"(value={val:.2f}, z={z:.2f})"
        )
        rows.append({
            "date":        ts.date().isoformat() if hasattr(ts, "date") else str(ts),
            "value":       round(val, 2),
            "zscore":      round(float(z), 4),
            "severity":    sev,
            "description": desc,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

def store_anomalies(anomalies_df: pd.DataFrame, db_path: str) -> int:
    """
    Persist detected anomalies to forecast_events.
    Returns number of new rows inserted.
    """
    if anomalies_df.empty:
        return 0

    conn = get_connection(db_path)
    # Avoid re-inserting anomalies for the same dates
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT date FROM forecast_events WHERE type = 'anomaly'"
        ).fetchall()
    }
    inserted = 0
    for _, row in anomalies_df.iterrows():
        if row["date"] not in existing:
            conn.execute(
                """INSERT INTO forecast_events (date, type, severity, description)
                   VALUES (?, 'anomaly', ?, ?)""",
                (row["date"], row["severity"], row["description"]),
            )
            inserted += 1
    conn.commit()
    conn.close()
    return inserted


def get_stored_events(db_path: str, event_type: Optional[str] = None) -> List[Dict]:
    """Return stored forecast events, optionally filtered by type."""
    conn = get_connection(db_path)
    if event_type:
        rows = conn.execute(
            "SELECT * FROM forecast_events WHERE type = ? ORDER BY date DESC",
            (event_type,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM forecast_events ORDER BY date DESC"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_anomaly_detection(db_path: str, threshold: float = _ROBUST_Z_THRESHOLD) -> Dict:
    """
    Load daily sales from the DB, detect anomalies, persist new ones, and
    return a summary dict.
    """
    conn = get_connection(db_path)
    query = (
        "SELECT date_id AS ds, SUM(total_amount) AS y "
        "FROM fact_sales GROUP BY date_id ORDER BY date_id"
    )
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty or len(df) < _MIN_ROWS:
        return {
            "status":    "insufficient_data",
            "anomalies": [],
            "message":   f"Need at least {_MIN_ROWS} days of data.",
        }

    df["ds"] = pd.to_datetime(df["ds"])
    series   = df.set_index("ds")["y"]
    full_idx = pd.date_range(series.index.min(), series.index.max(), freq="D")
    series   = series.reindex(full_idx, fill_value=0.0)

    anomalies_df = detect_anomalies(series, threshold=threshold)
    if not anomalies_df.empty:
        try:
            store_anomalies(anomalies_df, db_path)
        except Exception as exc:
            logger.warning("Failed to persist anomalies: %s", exc)

    return {
        "status":    "ok",
        "anomalies": anomalies_df.to_dict(orient="records"),
        "count":     len(anomalies_df),
    }
