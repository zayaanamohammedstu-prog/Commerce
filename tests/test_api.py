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
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as ptmp:
        platform_db_path = ptmp.name

    flask_app.config["TESTING"] = True
    flask_app.config["DATABASE"] = db_path
    flask_app.config["PLATFORM_DATABASE"] = platform_db_path

    # Seed the test database with real data
    run_etl(raw_dir=RAW_DIR, db_path=db_path)

    # Initialise a fresh platform DB for this test module
    from warehouse.platform_db import init_platform_db
    init_platform_db(platform_db_path)

    with flask_app.test_client() as client:
        yield client

    os.unlink(db_path)
    try:
        os.unlink(platform_db_path)
    except OSError:
        pass


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


# ── Auth & Registration ───────────────────────────────────────────────────────

def test_register_creates_pending_user(client):
    """POST /register creates a user with status='pending'."""
    res = client.post("/register", data={
        "email": "newclient@test.com",
        "password": "SecurePass1",
        "business_name": "Test Biz",
    }, follow_redirects=True)
    assert res.status_code == 200

    from warehouse.platform_db import get_platform_connection
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    row = conn.execute("SELECT * FROM app_users WHERE email='newclient@test.com'").fetchone()
    conn.close()
    assert row is not None
    assert row["status"] == "pending"
    assert row["role"] == "client"


def test_login_page_loads(client):
    res = client.get("/login")
    assert res.status_code == 200
    assert b"Login" in res.data


def test_register_page_loads(client):
    res = client.get("/register")
    assert res.status_code == 200
    assert b"Register" in res.data


def test_landing_page_loads(client):
    res = client.get("/")
    assert res.status_code == 200
    assert b"Commerce Analytics" in res.data


def test_about_page_loads(client):
    res = client.get("/about")
    assert res.status_code == 200


# ── Authorization: non-admin cannot access admin endpoints ────────────────────

def test_non_admin_cannot_access_admin_api(client):
    """Unauthenticated request to admin API → 401."""
    with client.session_transaction() as sess:
        sess.clear()
    res = client.get("/api/admin/users")
    assert res.status_code in (401, 403)


def test_non_admin_cannot_access_admin_page(client):
    """Unauthenticated request to admin page → redirect to login."""
    with client.session_transaction() as sess:
        sess.clear()
    res = client.get("/admin", follow_redirects=False)
    assert res.status_code in (302, 401, 403)


def test_pending_client_cannot_access_client_api(client):
    """Pending client cannot access /api/client/* endpoints."""
    from warehouse.platform_db import get_platform_connection
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    row = conn.execute("SELECT id FROM app_users WHERE email='newclient@test.com'").fetchone()
    conn.close()
    assert row is not None, "newclient@test.com must exist (created in test_register_creates_pending_user)"

    with client.session_transaction() as sess:
        sess["user_id"] = row["id"]
        sess["role"] = "client"

    res = client.get("/api/client/kpis")
    assert res.status_code == 403

    # Clean up session
    with client.session_transaction() as sess:
        sess.clear()


# ── Admin approval enables client endpoints ───────────────────────────────────

def _login_as_admin(client):
    """Helper: create a fresh admin session and return user id."""
    from werkzeug.security import generate_password_hash
    from warehouse.platform_db import get_platform_connection
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    # Ensure an admin exists
    row = conn.execute("SELECT id FROM app_users WHERE role='admin' LIMIT 1").fetchone()
    if not row:
        conn.execute(
            "INSERT INTO app_users (email, password_hash, role, status, business_name) "
            "VALUES ('admin@test.com', ?, 'admin', 'approved', 'Admin')",
            (generate_password_hash("Admin1234!"),)
        )
        conn.commit()
        row = conn.execute("SELECT id FROM app_users WHERE role='admin' LIMIT 1").fetchone()
    admin_id = row["id"]
    conn.close()
    with client.session_transaction() as sess:
        sess["user_id"] = admin_id
        sess["role"] = "admin"
    return admin_id


def test_admin_approval_enables_client_endpoints(client):
    """Admin approves pending user → client can access /api/client/* endpoints."""
    from warehouse.platform_db import get_platform_connection

    # Get pending user created earlier
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    row = conn.execute("SELECT id FROM app_users WHERE email='newclient@test.com'").fetchone()
    conn.close()
    assert row is not None
    pending_id = row["id"]

    # Login as admin and approve
    _login_as_admin(client)
    res = client.post(f"/api/admin/users/{pending_id}/approve")
    assert res.status_code == 200
    data = res.get_json()
    assert data["status"] in ("approved", "already_approved")

    # Verify the user is now approved
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    row = conn.execute("SELECT status FROM app_users WHERE id=?", (pending_id,)).fetchone()
    conn.close()
    assert row["status"] == "approved"

    # Login as approved client and access endpoint
    with client.session_transaction() as sess:
        sess["user_id"] = pending_id
        sess["role"] = "client"

    res = client.get("/api/client/kpis")
    assert res.status_code == 200

    with client.session_transaction() as sess:
        sess.clear()


def test_admin_can_create_user(client):
    """Admin POST /api/admin/users/create creates a new approved user."""
    _login_as_admin(client)
    res = client.post("/api/admin/users/create",
                      json={"email": "created@test.com", "password": "Pass1234!", "role": "client", "business_name": "Created Co"})
    assert res.status_code == 201
    data = res.get_json()
    assert data["status"] == "created"
    assert "user_id" in data

    with client.session_transaction() as sess:
        sess.clear()


def test_admin_list_users(client):
    """Admin GET /api/admin/users returns a list."""
    _login_as_admin(client)
    res = client.get("/api/admin/users")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0

    with client.session_transaction() as sess:
        sess.clear()


# ── Per-client DB isolation ───────────────────────────────────────────────────

def test_per_client_db_isolation(client):
    """Two approved clients have separate warehouse DBs with independent data."""
    import io
    from datetime import date, timedelta
    from warehouse.platform_db import get_platform_connection, create_client_db

    _login_as_admin(client)

    # Create two clients
    res1 = client.post("/api/admin/users/create",
                       json={"email": "client_a@test.com", "password": "PassA1234!", "role": "client", "business_name": "Client A"})
    assert res1.status_code == 201
    client_a_id = res1.get_json()["user_id"]

    res2 = client.post("/api/admin/users/create",
                       json={"email": "client_b@test.com", "password": "PassB1234!", "role": "client", "business_name": "Client B"})
    assert res2.status_code == 201
    client_b_id = res2.get_json()["user_id"]

    with client.session_transaction() as sess:
        sess.clear()

    # Client A uploads data
    start = date(2024, 1, 1)
    csv_a = io.BytesIO(("\n".join(
        ["date,revenue"] + [f"{start + timedelta(days=i)},{100 + i}" for i in range(30)]
    )).encode())

    with client.session_transaction() as sess:
        sess["user_id"] = client_a_id
        sess["role"] = "client"

    res = client.post("/api/client/upload",
                      data={"file": (csv_a, "sales_a.csv", "text/csv")},
                      content_type="multipart/form-data")
    assert res.status_code == 200
    counts_a = res.get_json()["rows_loaded"]
    assert counts_a["sales"] > 0

    with client.session_transaction() as sess:
        sess.clear()

    # Client B has no data
    with client.session_transaction() as sess:
        sess["user_id"] = client_b_id
        sess["role"] = "client"

    kpis_b = client.get("/api/client/kpis").get_json()
    # Client B should have 0 orders (separate empty DB)
    assert kpis_b.get("total_orders", 0) == 0

    with client.session_transaction() as sess:
        sess.clear()

    # Client A's KPIs should have data
    with client.session_transaction() as sess:
        sess["user_id"] = client_a_id
        sess["role"] = "client"

    kpis_a = client.get("/api/client/kpis").get_json()
    assert kpis_a.get("total_orders", 0) > 0

    with client.session_transaction() as sess:
        sess.clear()


# ── Client inactivity logout logic ───────────────────────────────────────────

def test_client_inactivity_logout(client):
    """A client inactive for 30+ days is logged out on next request."""
    from datetime import datetime, timedelta, timezone
    from warehouse.platform_db import get_platform_connection

    # Create an approved client
    _login_as_admin(client)
    res = client.post("/api/admin/users/create",
                      json={"email": "inactive@test.com", "password": "Inact1234!", "role": "client", "business_name": "Inactive Co"})
    assert res.status_code == 201
    inactive_id = res.get_json()["user_id"]
    with client.session_transaction() as sess:
        sess.clear()

    # Set last_activity_at to 31 days ago
    old_time = (datetime.now(timezone.utc) - timedelta(days=31)).replace(tzinfo=None).isoformat()
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    conn.execute("UPDATE app_users SET last_activity_at=? WHERE id=?", (old_time, inactive_id))
    conn.commit()
    conn.close()

    # Simulate client session
    with client.session_transaction() as sess:
        sess["user_id"] = inactive_id
        sess["role"] = "client"

    res = client.get("/api/client/kpis")
    assert res.status_code == 401
    data = res.get_json()
    assert "inactivity" in data.get("error", "").lower() or "expired" in data.get("error", "").lower()

    with client.session_transaction() as sess:
        sess.clear()


# ── New required route aliases ────────────────────────────────────────────────

def test_sales_trend_alias(client):
    """GET /api/sales/trend is an alias for /api/sales/timeseries."""
    res = client.get("/api/sales/trend?granularity=monthly")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "period" in data[0]
    assert "revenue" in data[0]


def test_top_products_alias(client):
    """GET /api/top-products is an alias for /api/products/top."""
    res = client.get("/api/top-products")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "name" in data[0]
    assert "revenue" in data[0]


def test_regions_alias(client):
    """GET /api/regions is an alias for /api/sales/region."""
    res = client.get("/api/regions")
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "region" in data[0]
    assert "revenue" in data[0]


# ── Forecast persistence ──────────────────────────────────────────────────────

def test_client_forecast_persists(client):
    """GET /api/client/forecast stores a forecast_run and forecast_values row."""
    import sqlite3
    from warehouse.platform_db import get_platform_connection, create_client_db

    # Create and approve a test client with a warehouse DB
    _login_as_admin(client)
    res = client.post(
        "/api/admin/users/create",
        json={
            "email": "forecast_persist@test.com",
            "password": "ForecastPass1!",
            "role": "client",
            "business_name": "Forecast Test Co",
        },
    )
    assert res.status_code == 201
    fp_id = res.get_json()["user_id"]

    with client.session_transaction() as sess:
        sess.clear()

    # Upload some data so forecasting has something to work with
    start = date(2024, 1, 1)
    csv_data = io.BytesIO(
        (
            "\n".join(
                ["date,revenue"]
                + [f"{start + timedelta(days=i)},{100 + i}" for i in range(60)]
            )
        ).encode()
    )

    with client.session_transaction() as sess:
        sess["user_id"] = fp_id
        sess["role"] = "client"

    upload_res = client.post(
        "/api/client/upload",
        data={"file": (csv_data, "sales.csv", "text/csv")},
        content_type="multipart/form-data",
    )
    assert upload_res.status_code == 200

    # Run forecast
    forecast_res = client.get("/api/client/forecast?horizon=7")
    assert forecast_res.status_code == 200
    data = forecast_res.get_json()
    assert len(data) == 7
    assert "date" in data[0]
    assert "forecast" in data[0]

    # Verify persistence in the client warehouse DB
    conn = get_platform_connection(flask_app.config["PLATFORM_DATABASE"])
    row = conn.execute(
        "SELECT db_path FROM app_clients WHERE user_id = ?", (fp_id,)
    ).fetchone()
    conn.close()
    assert row is not None, "Client DB should be created after approval"

    client_db_path = row["db_path"]
    wh_conn = sqlite3.connect(client_db_path)
    run_count = wh_conn.execute("SELECT COUNT(*) FROM forecast_runs").fetchone()[0]
    val_count = wh_conn.execute("SELECT COUNT(*) FROM forecast_values").fetchone()[0]
    wh_conn.close()

    assert run_count >= 1, "At least one forecast_run should be persisted"
    assert val_count >= 7, "At least 7 forecast_values should be persisted"

    with client.session_transaction() as sess:
        sess.clear()


# ── Export endpoints ──────────────────────────────────────────────────────────

def test_export_sales_csv_requires_auth(client):
    """/api/export/sales.csv requires authentication."""
    with client.session_transaction() as sess:
        sess.clear()
    res = client.get("/api/export/sales.csv")
    assert res.status_code in (401, 302)


def test_export_forecast_csv_no_data(client):
    """/api/export/forecast.csv returns 404 when no forecast has been run."""
    # Create a fresh client with no forecast data
    _login_as_admin(client)
    res = client.post(
        "/api/admin/users/create",
        json={
            "email": "no_forecast@test.com",
            "password": "NoForecast1!",
            "role": "client",
            "business_name": "No Forecast Co",
        },
    )
    assert res.status_code == 201
    nf_id = res.get_json()["user_id"]

    with client.session_transaction() as sess:
        sess.clear()
        sess["user_id"] = nf_id
        sess["role"] = "client"

    res = client.get("/api/export/forecast.csv")
    assert res.status_code == 404

    with client.session_transaction() as sess:
        sess.clear()


# ── /api/upload alias ─────────────────────────────────────────────────────────

def test_api_upload_requires_auth(client):
    """/api/upload requires an authenticated client session."""
    with client.session_transaction() as sess:
        sess.clear()
    res = client.post(
        "/api/upload",
        data={"file": (io.BytesIO(b"date,revenue\n2024-01-01,100"), "s.csv", "text/csv")},
        content_type="multipart/form-data",
    )
    assert res.status_code in (401, 302)
