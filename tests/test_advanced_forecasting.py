"""
tests/test_advanced_forecasting.py
Tests for ensemble forecasting, anomaly detection, data quality, and what-if simulation.
"""
import os
import sys
import sqlite3
import tempfile
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from warehouse.database import init_db, get_connection


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db_with_sales(n_days: int = 40) -> str:
    """Create a temp SQLite DB with synthetic daily sales data."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()
    init_db(db_path)
    conn = get_connection(db_path)
    rng = np.random.default_rng(0)
    base_date = date(2024, 1, 1)
    for i in range(n_days):
        d = (base_date + timedelta(days=i)).isoformat()
        revenue = 1000 + 200 * np.sin(2 * np.pi * i / 7) + rng.normal(0, 50)
        revenue = max(0.0, float(revenue))
        conn.execute(
            "INSERT OR IGNORE INTO dim_products (product_id,name,category,unit_price) VALUES (?,?,?,?)",
            ("P1","Widget","General",10.0)
        )
        conn.execute(
            "INSERT OR IGNORE INTO dim_customers (customer_id,name,region,country) VALUES (?,?,?,?)",
            ("C1","Test Customer","East","US")
        )
        conn.execute(
            "INSERT OR IGNORE INTO dim_date (date_id,year,quarter,month,month_name,week,day,day_name,is_weekend) VALUES (?,?,?,?,?,?,?,?,?)",
            (d, 2024, 1, 1, "January", 1, i%7+1, "Monday", 0)
        )
        sale_id = f"S{i}"
        conn.execute(
            "INSERT OR IGNORE INTO fact_sales (sale_id,date_id,product_id,customer_id,quantity,unit_price,total_amount,region) VALUES (?,?,?,?,?,?,?,?)",
            (sale_id, d, "P1", "C1", 10, 10.0, revenue, "East")
        )
    conn.commit()
    conn.close()
    return db_path


def _cleanup(db_path: str):
    try:
        os.unlink(db_path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Ensemble forecasting
# ---------------------------------------------------------------------------

class TestEnsembleForecasting:
    def test_output_shape(self):
        from models.ensemble_forecasting import ensemble_forecast
        db_path = _make_db_with_sales(40)
        try:
            result = ensemble_forecast(horizon=14, db_path=db_path)
            assert "forecast" in result
            assert len(result["forecast"]) == 14
        finally:
            _cleanup(db_path)

    def test_forecast_fields(self):
        from models.ensemble_forecasting import ensemble_forecast
        db_path = _make_db_with_sales(40)
        try:
            result = ensemble_forecast(horizon=7, db_path=db_path)
            for row in result["forecast"]:
                assert "date" in row
                assert "yhat" in row
                assert "yhat_lower" in row
                assert "yhat_upper" in row
                assert row["yhat_lower"] <= row["yhat"]
                assert row["yhat_upper"] >= row["yhat"] - 1e-6  # upper bound with float tolerance
        finally:
            _cleanup(db_path)

    def test_meta_fields(self):
        from models.ensemble_forecasting import ensemble_forecast
        db_path = _make_db_with_sales(40)
        try:
            result = ensemble_forecast(horizon=7, db_path=db_path)
            assert result["algorithm"] == "Ensemble"
            assert isinstance(result["weights"], dict)
            assert isinstance(result["errors"], list)
            assert result["training_days"] > 0
        finally:
            _cleanup(db_path)

    def test_empty_db_fallback(self):
        from models.ensemble_forecasting import ensemble_forecast
        db_path = _make_db_with_sales(0)
        try:
            result = ensemble_forecast(horizon=7, db_path=db_path)
            assert len(result["forecast"]) == 7
            assert len(result["errors"]) > 0
        finally:
            _cleanup(db_path)

    def test_weights_sum_to_one(self):
        from models.ensemble_forecasting import ensemble_forecast
        db_path = _make_db_with_sales(40)
        try:
            result = ensemble_forecast(horizon=7, db_path=db_path)
            total = sum(result["weights"].values())
            assert abs(total - 1.0) < 1e-3
        finally:
            _cleanup(db_path)


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

class TestAnomalyDetection:
    def test_detect_spike(self):
        from models.anomaly import detect_anomalies
        rng = np.random.default_rng(1)
        idx = pd.date_range("2024-01-01", periods=60, freq="D")
        values = pd.Series(1000 + rng.normal(0, 20, 60), index=idx)
        values.iloc[30] = 5000  # inject spike
        result = detect_anomalies(values)
        assert len(result) >= 1
        assert "2024-01-31" in result["date"].values

    def test_no_anomalies_clean_data(self):
        from models.anomaly import detect_anomalies
        idx = pd.date_range("2024-01-01", periods=30, freq="D")
        values = pd.Series([1000.0] * 30, index=idx)
        result = detect_anomalies(values)
        assert len(result) == 0

    def test_run_from_db(self):
        from models.anomaly import run_anomaly_detection
        db_path = _make_db_with_sales(30)
        try:
            result = run_anomaly_detection(db_path)
            assert "status" in result
            assert "anomalies" in result
        finally:
            _cleanup(db_path)

    def test_insufficient_data(self):
        from models.anomaly import run_anomaly_detection
        db_path = _make_db_with_sales(3)
        try:
            result = run_anomaly_detection(db_path)
            assert result["status"] == "insufficient_data"
        finally:
            _cleanup(db_path)


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

class TestDataQuality:
    def _good_df(self, n=30):
        rng = np.random.default_rng(2)
        dates = pd.date_range("2024-01-01", periods=n, freq="D")
        return pd.DataFrame({"date": dates.astype(str), "revenue": rng.uniform(500, 1500, n)})

    def test_good_df_high_score(self):
        from models.data_quality import validate_dataframe
        df = self._good_df(30)
        result = validate_dataframe(df)
        assert result["data_quality_score"] >= 70

    def test_missing_date_col(self):
        from models.data_quality import validate_dataframe
        df = pd.DataFrame({"sales": [100, 200, 300]})
        result = validate_dataframe(df)
        assert result["data_quality_score"] <= 60
        assert result["clean_df"] is None

    def test_synonyms_detected(self):
        from models.data_quality import validate_dataframe
        df = pd.DataFrame({
            "order_date": pd.date_range("2024-01-01", periods=20).astype(str),
            "total_sales": [500.0] * 20,
        })
        result = validate_dataframe(df)
        assert result["resolved_date_col"] == "order_date"
        assert result["resolved_revenue_col"] == "total_sales"

    def test_currency_stripped(self):
        from models.data_quality import validate_dataframe
        df = pd.DataFrame({
            "date":    pd.date_range("2024-01-01", periods=20).astype(str),
            "revenue": ["$1,234.00"] * 20,
        })
        result = validate_dataframe(df)
        assert result["data_quality_score"] > 0
        assert result["clean_df"] is not None

    def test_negative_revenue_penalised(self):
        from models.data_quality import validate_dataframe
        df = self._good_df(20)
        df.loc[0, "revenue"] = -500.0
        result = validate_dataframe(df)
        assert any("negative" in w.lower() for w in result["warnings"])

    def test_score_from_db(self):
        from models.data_quality import score_from_db
        db_path = _make_db_with_sales(30)
        try:
            score = score_from_db(db_path)
            assert 0 <= score <= 100
        finally:
            _cleanup(db_path)


# ---------------------------------------------------------------------------
# What-if simulation
# ---------------------------------------------------------------------------

class TestWhatIf:
    def _base_forecast(self, n=30):
        today = date.today()
        return [
            {
                "date":       (today + timedelta(days=i+1)).isoformat(),
                "yhat":       1000.0,
                "yhat_lower": 850.0,
                "yhat_upper": 1150.0,
            }
            for i in range(n)
        ]

    def test_output_shape(self):
        from models.whatif import run_whatif
        base = self._base_forecast(14)
        result = run_whatif(base, {})
        assert len(result) == 14

    def test_corridor_fields(self):
        from models.whatif import run_whatif
        base = self._base_forecast(14)
        result = run_whatif(base, {"promo_boost_pct": 20})
        for row in result:
            assert "date" in row
            assert "p10" in row
            assert "p50" in row
            assert "p90" in row
            assert row["p10"] <= row["p50"] <= row["p90"]

    def test_promo_boost_increases_median(self):
        from models.whatif import run_whatif
        base = self._base_forecast(14)
        baseline = run_whatif(base, {})
        boosted  = run_whatif(base, {"promo_boost_pct": 30})
        base_med    = sum(r["p50"] for r in baseline)
        boosted_med = sum(r["p50"] for r in boosted)
        assert boosted_med > base_med

    def test_price_increase_effect(self):
        from models.whatif import run_whatif
        base = self._base_forecast(14)
        # Small price increase (positive elasticity effect expected to lower demand)
        result = run_whatif(base, {"price_change_pct": 10})
        assert all(r["p50"] >= 0 for r in result)

    def test_empty_base_returns_empty(self):
        from models.whatif import run_whatif
        result = run_whatif([], {"promo_boost_pct": 10})
        assert result == []
