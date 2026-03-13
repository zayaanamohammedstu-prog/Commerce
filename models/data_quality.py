"""
models/data_quality.py
Data Quality Guardian for ShopIntel.

Validates uploaded DataFrames and computes a 0-100 data-quality score.
The score is attached to forecast runs and insights as a confidence indicator.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Column name synonyms
# ---------------------------------------------------------------------------
_DATE_SYNONYMS = [
    "date", "order_date", "transaction_date", "sale_date",
    "purchase_date", "timestamp", "time", "created_at", "ordered_at",
]
_REVENUE_SYNONYMS = [
    "revenue", "total_amount", "total", "sales", "amount",
    "total_sales", "sale_amount", "net_sales", "gross_sales",
    "subtotal", "line_total", "price_total",
]
_QTY_SYNONYMS   = ["quantity", "qty", "units", "count", "num_units"]
_PRICE_SYNONYMS = ["unit_price", "price", "unit_cost", "item_price", "cost"]


def _normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + strip column names."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df


def _find_col(df: pd.DataFrame, synonyms: List[str]) -> Optional[str]:
    for s in synonyms:
        if s in df.columns:
            return s
    return None


def _strip_currency(series: pd.Series) -> pd.Series:
    """Remove currency symbols and commas before numeric conversion."""
    if series.dtype == object:
        series = series.str.replace(r"[\$£€,]", "", regex=True).str.strip()
    return pd.to_numeric(series, errors="coerce")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame) -> Dict:
    """
    Validate an uploaded DataFrame.

    Returns:
      {
        "data_quality_score": int (0-100),
        "warnings": [...],
        "suggestions": [...],
        "resolved_date_col": str | None,
        "resolved_revenue_col": str | None,
        "clean_df": pd.DataFrame | None,
      }
    """
    df = _normalise_cols(df)
    warnings: List[str] = []
    suggestions: List[str] = []
    score = 100

    # -- Find date column --
    date_col = _find_col(df, _DATE_SYNONYMS)
    if date_col is None:
        warnings.append("No date column found.")
        suggestions.append(
            f"Rename your date column to one of: {', '.join(_DATE_SYNONYMS[:5])}."
        )
        score -= 40

    # -- Find revenue column --
    rev_col = _find_col(df, _REVENUE_SYNONYMS)
    if rev_col is None:
        qty_col   = _find_col(df, _QTY_SYNONYMS)
        price_col = _find_col(df, _PRICE_SYNONYMS)
        if qty_col and price_col:
            df["revenue"] = (
                _strip_currency(df[qty_col]) * _strip_currency(df[price_col])
            )
            rev_col = "revenue"
            warnings.append(
                f"Revenue derived from '{qty_col}' × '{price_col}'."
            )
        else:
            warnings.append("No revenue column found.")
            suggestions.append(
                "Add a 'revenue' column or both 'quantity' and 'unit_price' columns."
            )
            score -= 40

    if date_col is None or rev_col is None:
        return {
            "data_quality_score": max(0, score),
            "warnings": warnings,
            "suggestions": suggestions,
            "resolved_date_col": date_col,
            "resolved_revenue_col": rev_col,
            "clean_df": None,
        }

    # -- Parse dates --
    df["_date"] = pd.to_datetime(df[date_col], errors="coerce")
    n_bad_dates = df["_date"].isna().sum()
    if n_bad_dates > 0:
        pct = n_bad_dates / len(df) * 100
        warnings.append(f"{n_bad_dates} rows ({pct:.1f}%) have unparseable dates.")
        suggestions.append("Ensure dates are in YYYY-MM-DD or MM/DD/YYYY format.")
        score -= min(20, int(pct * 0.5))

    # -- Parse revenue --
    df["_rev"] = _strip_currency(df[rev_col])
    n_bad_rev = df["_rev"].isna().sum()
    if n_bad_rev > 0:
        pct = n_bad_rev / len(df) * 100
        warnings.append(f"{n_bad_rev} rows ({pct:.1f}%) have non-numeric revenue.")
        suggestions.append("Remove currency symbols or text from revenue column.")
        score -= min(15, int(pct * 0.5))

    # -- Drop invalid rows --
    clean = df.dropna(subset=["_date", "_rev"]).copy()

    # -- Negative values --
    neg_mask = clean["_rev"] < 0
    if neg_mask.any():
        n_neg = neg_mask.sum()
        warnings.append(
            f"{n_neg} rows have negative revenue (returns/refunds?). "
            "They will be treated as 0."
        )
        suggestions.append(
            "If these are returns, consider filtering them out before uploading."
        )
        clean.loc[neg_mask, "_rev"] = 0.0
        score -= min(10, n_neg)

    # -- Duplicates --
    n_dup = clean.duplicated(subset=["_date"]).sum()
    if n_dup > 0:
        warnings.append(
            f"{n_dup} duplicate dates found; revenue will be summed per day."
        )
        score -= min(5, n_dup // 2)

    # -- Sparsity (zero revenue days) --
    daily = clean.groupby("_date")["_rev"].sum()
    full_idx  = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
    daily_full = daily.reindex(full_idx, fill_value=0.0)
    zero_pct = (daily_full == 0).mean() * 100
    if zero_pct > 50:
        warnings.append(
            f"{zero_pct:.0f}% of calendar days have zero revenue "
            "(sparse data – forecast quality will be reduced)."
        )
        suggestions.append(
            "Consider only uploading days where transactions occurred, "
            "or verify there are no missing records."
        )
        score -= min(15, int(zero_pct * 0.2))

    # -- Minimum length --
    n_days = len(daily_full)
    if n_days < 14:
        warnings.append(
            f"Only {n_days} days of data available. "
            "Forecasting requires at least 14 days for reliable results."
        )
        suggestions.append("Upload more historical data for better forecasts.")
        score -= 15

    # -- Sudden spikes (potential data entry errors) --
    if len(daily_full) >= 7:
        from models.anomaly import _robust_zscore
        zscores = _robust_zscore(daily_full)
        extreme = (zscores > 5).sum()
        if extreme > 0:
            warnings.append(
                f"{extreme} day(s) have extreme revenue spikes "
                "(robust z-score > 5). Check for data entry errors."
            )
            score -= min(10, extreme * 2)

    score = max(0, min(100, score))

    # Build clean dataframe
    clean_df = pd.DataFrame({
        "date":    clean["_date"],
        "revenue": clean["_rev"],
    }).dropna()

    return {
        "data_quality_score":  score,
        "warnings":            warnings,
        "suggestions":         suggestions,
        "resolved_date_col":   date_col,
        "resolved_revenue_col": rev_col,
        "clean_df":            clean_df,
    }


def score_from_db(db_path: str) -> float:
    """Compute a data quality score for data already loaded into the warehouse."""
    try:
        from warehouse.database import get_connection
        conn = get_connection(db_path)
        df = pd.read_sql(
            "SELECT date_id AS date, SUM(total_amount) AS revenue "
            "FROM fact_sales GROUP BY date_id ORDER BY date_id",
            conn,
        )
        conn.close()
        if df.empty:
            return 0.0
        df["date"] = pd.to_datetime(df["date"])
        result = validate_dataframe(df)
        return float(result["data_quality_score"])
    except Exception:
        return 50.0
