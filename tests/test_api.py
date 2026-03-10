"""
tests/test_api.py
Integration tests for the Flask REST API endpoints.
"""
import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.app import app as flask_app
from etl.load import run_etl

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


@pytest.fixture(scope="module")
def client():
    """Create a Flask test client backed by an in-memory test database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    flask_app.config["TESTING"] = True
    flask_app.config["DATABASE"] = db_path

    # Seed the test database with real data
    run_etl(raw_dir=RAW_DIR, db_path=db_path)

    with flask_app.test_client() as client:
        yield client

    os.unlink(db_path)


# ── Web route ─────────────────────────────────────────────────────────────────

def test_index_returns_html(client):
    res = client.get("/")
    assert res.status_code == 200
    assert b"Commerce Analytics" in res.data


# ── KPIs ─────────────────────────────────────────────────────────────────────

def test_kpis_status(client):
    res = client.get("/api/kpis")
    assert res.status_code == 200


def test_kpis_fields(client):
    data = client.get("/api/kpis").get_json()
    for field in ("total_orders", "total_revenue", "avg_order_value",
                  "total_units_sold", "unique_customers", "unique_products"):
        assert field in data, f"Missing field: {field}"


def test_kpis_positive_revenue(client):
    data = client.get("/api/kpis").get_json()
    assert data["total_revenue"] > 0


# ── Sales Timeseries ─────────────────────────────────────────────────────────

def test_sales_timeseries_monthly(client):
    res = client.get("/api/sales/timeseries?granularity=monthly")
    assert res.status_code == 200
    data = res.get_json()
    assert len(data) > 0
    assert "period" in data[0]
    assert "revenue" in data[0]


def test_sales_timeseries_weekly(client):
    res = client.get("/api/sales/timeseries?granularity=weekly")
    assert res.status_code == 200
    assert len(res.get_json()) > 0


def test_sales_timeseries_daily(client):
    res = client.get("/api/sales/timeseries?granularity=daily")
    assert res.status_code == 200
    assert len(res.get_json()) > 0


# ── Top Products ──────────────────────────────────────────────────────────────

def test_top_products_returns_list(client):
    res = client.get("/api/products/top")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) <= 10


def test_top_products_has_fields(client):
    data = client.get("/api/products/top").get_json()
    assert "name" in data[0]
    assert "revenue" in data[0]


def test_top_products_custom_limit(client):
    data = client.get("/api/products/top?limit=5").get_json()
    assert len(data) <= 5


# ── Sales by Region ───────────────────────────────────────────────────────────

def test_sales_region_returns_list(client):
    res = client.get("/api/sales/region")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0


def test_sales_region_has_fields(client):
    data = client.get("/api/sales/region").get_json()
    for field in ("region", "revenue", "order_count"):
        assert field in data[0], f"Missing: {field}"


# ── Sales by Category ─────────────────────────────────────────────────────────

def test_sales_category(client):
    res = client.get("/api/sales/category")
    assert res.status_code == 200
    data = res.get_json()
    assert len(data) > 0
    assert "category" in data[0]


# ── Forecast ─────────────────────────────────────────────────────────────────

def test_forecast_default_horizon(client):
    res = client.get("/api/forecast")
    assert res.status_code == 200
    data = res.get_json()
    assert len(data) == 30
    assert "date" in data[0]
    assert "forecast" in data[0]


def test_forecast_custom_horizon(client):
    res = client.get("/api/forecast?horizon=14")
    data = res.get_json()
    assert len(data) == 14


def test_forecast_values_non_negative(client):
    data = client.get("/api/forecast").get_json()
    assert all(d["forecast"] >= 0 for d in data)


def test_forecast_summary(client):
    res = client.get("/api/forecast/summary")
    assert res.status_code == 200
    data = res.get_json()
    assert data["status"] == "ready"
    assert "algorithm" in data


# ── Customers ─────────────────────────────────────────────────────────────────

def test_customers_endpoint(client):
    res = client.get("/api/customers")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0


# ── ETL trigger ───────────────────────────────────────────────────────────────

def test_etl_trigger_idempotent(client):
    """Triggering ETL via API on an already-loaded DB returns 0 new rows."""
    res = client.post("/api/etl/run")
    assert res.status_code == 200
    data = res.get_json()
    assert data["status"] == "success"
    assert data["rows_loaded"]["sales"] == 0


# ── Admin: Upload prediction ───────────────────────────────────────────────────

import io
from datetime import date, timedelta


def _make_csv(rows, header="date,revenue"):
    """Build an in-memory CSV bytes object."""
    lines = [header] + [f"{r[0]},{r[1]}" for r in rows]
    return io.BytesIO("\n".join(lines).encode())


def test_predict_upload_success(client):
    """Upload a valid CSV → 200 with forecast list."""
    start = date(2024, 1, 1)
    rows  = [(str(start + timedelta(days=i)), 100 + i * 3) for i in range(20)]
    csv_data = _make_csv(rows)
    res = client.post(
        "/api/admin/predict/upload",
        data={"file": (csv_data, "sales.csv", "text/csv"), "horizon": "7"},
        content_type="multipart/form-data",
    )
    assert res.status_code == 200
    data = res.get_json()
    assert "forecast" in data
    assert len(data["forecast"]) == 7
    assert "date"     in data["forecast"][0]
    assert "forecast" in data["forecast"][0]
    assert data["horizon"] == 7
    assert "training_days"       in data
    assert "mean_daily_revenue"  in data


def test_predict_upload_missing_file(client):
    """No file part → 400."""
    res = client.post("/api/admin/predict/upload", data={}, content_type="multipart/form-data")
    assert res.status_code == 400
    assert "error" in res.get_json()


def test_predict_upload_invalid_extension(client):
    """Non-CSV file → 400."""
    res = client.post(
        "/api/admin/predict/upload",
        data={"file": (io.BytesIO(b"col1,col2\n1,2"), "data.txt", "text/plain")},
        content_type="multipart/form-data",
    )
    assert res.status_code == 400
    assert "error" in res.get_json()


def test_predict_upload_missing_required_column(client):
    """CSV without date column → 422."""
    csv_data = io.BytesIO(b"amount,qty\n100,2\n200,3\n300,4")
    res = client.post(
        "/api/admin/predict/upload",
        data={"file": (csv_data, "bad.csv", "text/csv")},
        content_type="multipart/form-data",
    )
    assert res.status_code == 422
    assert "error" in res.get_json()


def test_predict_upload_total_amount_column(client):
    """CSV with total_amount column (alias for revenue) → 200."""
    start = date(2024, 3, 1)
    rows  = [(str(start + timedelta(days=i)), 250 + i * 5) for i in range(20)]
    csv_data = _make_csv(rows, header="date,total_amount")
    res = client.post(
        "/api/admin/predict/upload",
        data={"file": (csv_data, "totals.csv", "text/csv"), "horizon": "14"},
        content_type="multipart/form-data",
    )
    assert res.status_code == 200
    data = res.get_json()
    assert len(data["forecast"]) == 14
