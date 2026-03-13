"""
api/app.py
3-fold Flask web application:
  - Main (public): /, /about, /login, /register
  - Client portal: /client/*  (approved clients only)
  - Admin portal:  /admin/*   (admins only)

Authentication: session-based with role checks.
Per-client SQLite DB for warehouse data.
"""
import os
import sys
import uuid
import functools
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import (Flask, jsonify, request, render_template, redirect,
                   url_for, session, flash, abort, send_file)
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import pandas as pd

from warehouse.database import get_connection, DB_PATH
from warehouse.platform_db import (
    get_platform_connection, init_platform_db,
    get_client_db_path, create_client_db, PLATFORM_DB_PATH,
)

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "templates")
STATIC_DIR   = os.path.join(os.path.dirname(__file__), "..", "frontend", "static")
UPLOAD_DIR   = os.path.join(os.path.dirname(__file__), "..", "uploads")
_MAX_UPLOAD_BYTES = 10 * 1024 * 1024   # 10 MB
_INACTIVITY_DAYS  = 30

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)
# SECRET_KEY must be set as an environment variable in production to persist sessions across restarts
_DEFAULT_SECRET = "commerce-dev-secret-key-change-in-production"
app.secret_key = os.environ.get("SECRET_KEY", _DEFAULT_SECRET)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
CORS(app)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _platform_db():
    path = app.config.get("PLATFORM_DATABASE", PLATFORM_DB_PATH)
    return get_platform_connection(path)

def _get_db(db_path: str = None):
    path = db_path or app.config.get("DATABASE", DB_PATH)
    return get_connection(path)

def _df(query: str, params=(), db_path: str = None) -> pd.DataFrame:
    conn = _get_db(db_path)
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def _get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    conn = _platform_db()
    row = conn.execute("SELECT * FROM app_users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def _get_client_db(user_id: int = None) -> str:
    uid = user_id or session.get("user_id")
    conn = _platform_db()
    row = conn.execute("SELECT db_path FROM app_clients WHERE user_id = ?", (uid,)).fetchone()
    conn.close()
    if not row:
        raise ValueError(f"No client DB found for user {uid}")
    return row["db_path"]

# ---------------------------------------------------------------------------
# Auth decorators
# ---------------------------------------------------------------------------

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated

def client_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect(url_for("login"))
        user = _get_current_user()
        if not user:
            session.clear()
            return redirect(url_for("login"))
        if user["role"] != "client" and user["role"] != "admin":
            abort(403)
        if user["role"] == "client" and user["status"] == "pending":
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Account pending approval"}), 403
            return redirect(url_for("client_pending"))
        if user["role"] == "client" and user["status"] == "disabled":
            session.clear()
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Account disabled"}), 403
            flash("Your account has been disabled.", "error")
            return redirect(url_for("login"))
        # Inactivity check for clients
        if user["role"] == "client":
            last = user.get("last_activity_at")
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    # Make naive datetimes timezone-aware for comparison
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    if (datetime.now(timezone.utc) - last_dt).days >= _INACTIVITY_DAYS:
                        session.clear()
                        if request.is_json or request.path.startswith("/api/"):
                            return jsonify({"error": "Session expired due to inactivity"}), 401
                        flash("You have been logged out due to 30 days of inactivity.", "warning")
                        return redirect(url_for("login"))
                except (ValueError, TypeError):
                    pass
            # Update activity timestamp
            conn = _platform_db()
            conn.execute("UPDATE app_users SET last_activity_at = ? WHERE id = ?",
                         (datetime.now(timezone.utc).isoformat(), user["id"]))
            conn.commit()
            conn.close()
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect(url_for("login"))
        user = _get_current_user()
        if not user or user["role"] != "admin":
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Admin access required"}), 403
            abort(403)
        return f(*args, **kwargs)
    return decorated

# ---------------------------------------------------------------------------
# Startup: initialise platform DB + create default admin
# ---------------------------------------------------------------------------

def _ensure_platform_db():
    path = app.config.get("PLATFORM_DATABASE", PLATFORM_DB_PATH)
    init_platform_db(path)
    conn = get_platform_connection(path)
    admin = conn.execute("SELECT id FROM app_users WHERE role = 'admin' LIMIT 1").fetchone()
    if not admin:
        conn.execute(
            "INSERT INTO app_users (email, password_hash, role, status, business_name) VALUES (?, ?, 'admin', 'approved', 'Platform Admin')",
            ("admin@commerce.local", generate_password_hash("Admin1234!"))
        )
        conn.commit()
    conn.close()

with app.app_context():
    _ensure_platform_db()

# ---------------------------------------------------------------------------
# Main (public) routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    user = _get_current_user()
    return render_template("landing.html", user=user)

@app.route("/about")
def about():
    user = _get_current_user()
    return render_template("about.html", user=user)

@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        user = _get_current_user()
        if user:
            return redirect(url_for("admin_dashboard") if user["role"] == "admin" else url_for("client_dashboard"))

    if request.method == "POST":
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        conn = _platform_db()
        row = conn.execute("SELECT * FROM app_users WHERE email = ?", (email,)).fetchone()
        conn.close()
        if not row or not check_password_hash(row["password_hash"], password):
            flash("Invalid email or password.", "error")
            return render_template("login.html")
        user = dict(row)
        if user["status"] == "disabled":
            flash("Your account has been disabled.", "error")
            return render_template("login.html")

        session.clear()
        session["user_id"] = user["id"]
        session["role"]    = user["role"]
        session.permanent  = True
        app.permanent_session_lifetime = timedelta(days=_INACTIVITY_DAYS + 1)

        # Update last_login_at
        conn = _platform_db()
        conn.execute("UPDATE app_users SET last_login_at = ?, last_activity_at = ? WHERE id = ?",
                     (datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(), user["id"]))
        conn.commit()
        conn.close()

        if user["role"] == "admin":
            return redirect(url_for("admin_dashboard"))
        if user["status"] == "pending":
            return redirect(url_for("client_pending"))
        return redirect(url_for("client_dashboard"))

    return render_template("login.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        business = request.form.get("business_name", "").strip()

        if not email or not password or not business:
            flash("All fields are required.", "error")
            return render_template("register.html")
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("register.html")

        conn = _platform_db()
        existing = conn.execute("SELECT id FROM app_users WHERE email = ?", (email,)).fetchone()
        if existing:
            conn.close()
            flash("An account with that email already exists.", "error")
            return render_template("register.html")

        conn.execute(
            "INSERT INTO app_users (email, password_hash, role, status, business_name) VALUES (?, ?, 'client', 'pending', ?)",
            (email, generate_password_hash(password), business)
        )
        conn.commit()
        conn.close()
        flash("Registration successful! Your account is pending admin approval.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))

# ---------------------------------------------------------------------------
# Client pending page
# ---------------------------------------------------------------------------

@app.route("/client/pending")
@login_required
def client_pending():
    user = _get_current_user()
    if user and user["status"] == "approved":
        return redirect(url_for("client_dashboard"))
    return render_template("client_pending.html", user=user)

# ---------------------------------------------------------------------------
# Client portal routes
# ---------------------------------------------------------------------------

@app.route("/client")
@client_required
def client_dashboard():
    user = _get_current_user()
    try:
        db_path = _get_client_db(user["id"])
        conn = _get_db(db_path)
        row = conn.execute("SELECT COUNT(*) AS cnt, SUM(total_amount) AS rev FROM fact_sales").fetchone()
        conn.close()
        has_data = row and row["cnt"] and row["cnt"] > 0
    except Exception:
        has_data = False
    return render_template("client_dashboard.html", user=user, has_data=has_data)

@app.route("/client/upload")
@client_required
def client_upload():
    user = _get_current_user()
    return render_template("client_upload.html", user=user)

@app.route("/client/analytics")
@client_required
def client_analytics():
    user = _get_current_user()
    return render_template("client_analytics.html", user=user)

@app.route("/client/forecast")
@client_required
def client_forecast():
    user = _get_current_user()
    return render_template("client_forecast.html", user=user)

@app.route("/client/reports")
@client_required
def client_reports():
    user = _get_current_user()
    return render_template("client_reports.html", user=user)

# ---------------------------------------------------------------------------
# Admin portal routes
# ---------------------------------------------------------------------------

@app.route("/admin")
@admin_required
def admin_dashboard():
    user = _get_current_user()
    conn = _platform_db()
    stats = {
        "total_users": conn.execute("SELECT COUNT(*) FROM app_users WHERE role='client'").fetchone()[0],
        "pending": conn.execute("SELECT COUNT(*) FROM app_users WHERE status='pending'").fetchone()[0],
        "approved": conn.execute("SELECT COUNT(*) FROM app_users WHERE status='approved' AND role='client'").fetchone()[0],
        "disabled": conn.execute("SELECT COUNT(*) FROM app_users WHERE status='disabled'").fetchone()[0],
    }
    conn.close()
    return render_template("admin_dashboard.html", user=user, stats=stats)

@app.route("/admin/users")
@admin_required
def admin_users():
    user = _get_current_user()
    conn = _platform_db()
    users = [dict(r) for r in conn.execute("SELECT * FROM app_users ORDER BY created_at DESC").fetchall()]
    conn.close()
    return render_template("admin_users.html", user=user, users=users)

@app.route("/admin/profile", methods=["GET", "POST"])
@admin_required
def admin_profile():
    user = _get_current_user()
    if request.method == "POST":
        business_name = request.form.get("business_name", "").strip()
        new_password  = request.form.get("new_password", "")
        conn = _platform_db()
        if new_password:
            if len(new_password) < 8:
                flash("Password must be at least 8 characters.", "error")
                return render_template("admin_profile.html", user=user)
            conn.execute("UPDATE app_users SET password_hash=?, business_name=? WHERE id=?",
                         (generate_password_hash(new_password), business_name, user["id"]))
        else:
            conn.execute("UPDATE app_users SET business_name=? WHERE id=?",
                         (business_name, user["id"]))
        conn.commit()
        conn.close()
        flash("Profile updated.", "success")
        return redirect(url_for("admin_profile"))
    return render_template("admin_profile.html", user=user)

@app.route("/admin/analytics")
@admin_required
def admin_analytics():
    user = _get_current_user()
    conn = _platform_db()
    clients = [dict(r) for r in conn.execute(
        "SELECT u.id, u.email, u.business_name, u.status, u.created_at, u.last_activity_at, c.db_path "
        "FROM app_users u LEFT JOIN app_clients c ON u.id = c.user_id "
        "WHERE u.role = 'client' ORDER BY u.created_at DESC"
    ).fetchall()]
    conn.close()
    return render_template("admin_analytics.html", user=user, clients=clients)

# ---------------------------------------------------------------------------
# Admin API: user management
# ---------------------------------------------------------------------------

@app.route("/api/admin/users/pending")
@admin_required
def api_admin_pending_users():
    conn = _platform_db()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, email, business_name, created_at FROM app_users WHERE status='pending' ORDER BY created_at DESC"
    ).fetchall()]
    conn.close()
    return jsonify(rows)

@app.route("/api/admin/users", methods=["GET"])
@admin_required
def api_admin_list_users():
    conn = _platform_db()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, email, role, status, business_name, created_at, last_login_at FROM app_users ORDER BY created_at DESC"
    ).fetchall()]
    conn.close()
    return jsonify(rows)

@app.route("/api/admin/users/create", methods=["POST"])
@admin_required
def api_admin_create_user():
    data = request.get_json() or request.form
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    role     = data.get("role", "client")
    business = data.get("business_name", "").strip()

    if not email or not password:
        return jsonify({"error": "email and password are required"}), 400
    if role not in ("admin", "client"):
        return jsonify({"error": "role must be admin or client"}), 400

    conn = _platform_db()
    existing = conn.execute("SELECT id FROM app_users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        return jsonify({"error": "Email already registered"}), 409

    status = "approved"
    conn.execute(
        "INSERT INTO app_users (email, password_hash, role, status, business_name, approved_at) VALUES (?, ?, ?, ?, ?, ?)",
        (email, generate_password_hash(password), role, status, business, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    user_id = conn.execute("SELECT id FROM app_users WHERE email = ?", (email,)).fetchone()["id"]

    if role == "client":
        db_path = create_client_db(user_id)
        conn.execute("INSERT INTO app_clients (user_id, db_path) VALUES (?, ?)", (user_id, db_path))
        conn.commit()

    conn.close()
    return jsonify({"status": "created", "user_id": user_id}), 201

@app.route("/api/admin/users/<int:user_id>/approve", methods=["POST"])
@admin_required
def api_admin_approve_user(user_id):
    conn = _platform_db()
    row = conn.execute("SELECT * FROM app_users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "User not found"}), 404
    if row["status"] == "approved":
        conn.close()
        return jsonify({"status": "already_approved"}), 200

    conn.execute("UPDATE app_users SET status='approved', approved_at=? WHERE id=?",
                 (datetime.now(timezone.utc).isoformat(), user_id))
    conn.commit()

    # Create client DB
    existing_client = conn.execute("SELECT id FROM app_clients WHERE user_id = ?", (user_id,)).fetchone()
    if not existing_client:
        db_path = create_client_db(user_id)
        conn.execute("INSERT INTO app_clients (user_id, db_path) VALUES (?, ?)", (user_id, db_path))
        conn.commit()
    conn.close()
    return jsonify({"status": "approved"})

@app.route("/api/admin/users/<int:user_id>/disable", methods=["POST"])
@admin_required
def api_admin_disable_user(user_id):
    current = _get_current_user()
    if current["id"] == user_id:
        return jsonify({"error": "Cannot disable yourself"}), 400
    conn = _platform_db()
    conn.execute("UPDATE app_users SET status='disabled' WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "disabled"})

@app.route("/api/admin/users/<int:user_id>/enable", methods=["POST"])
@admin_required
def api_admin_enable_user(user_id):
    conn = _platform_db()
    conn.execute("UPDATE app_users SET status='approved' WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "enabled"})

@app.route("/api/admin/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def api_admin_reset_password(user_id):
    data = request.get_json() or request.form
    new_password = data.get("new_password") or ""
    if len(new_password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400
    conn = _platform_db()
    conn.execute("UPDATE app_users SET password_hash=? WHERE id=?",
                 (generate_password_hash(new_password), user_id))
    conn.commit()
    conn.close()
    return jsonify({"status": "password_reset"})

# ---------------------------------------------------------------------------
# Client API: upload + ETL
# ---------------------------------------------------------------------------

@app.route("/api/client/upload", methods=["POST"])
@client_required
def api_client_upload():
    from etl.client_pipeline import run_client_etl, allowed_file, MAX_UPLOAD_BYTES

    user = _get_current_user()
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "No file selected"}), 400

    filename = secure_filename(file.filename)
    if not allowed_file(filename):
        return jsonify({"error": "Unsupported file type. Use .csv, .xlsx, or .xls"}), 400

    file.seek(0, 2)
    size = file.tell()
    file.seek(0)
    if size > MAX_UPLOAD_BYTES:
        return jsonify({"error": f"File too large (max {MAX_UPLOAD_BYTES // (1024*1024)} MB)"}), 400

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    save_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex[:8]}_{filename}")
    file.save(save_path)

    try:
        db_path = _get_client_db(user["id"])
        counts = run_client_etl(save_path, db_path)
        return jsonify({"status": "success", "rows_loaded": counts})
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        return jsonify({"error": f"ETL error: {str(e)}"}), 500
    finally:
        try:
            os.remove(save_path)
        except OSError:
            pass

# ---------------------------------------------------------------------------
# Client API: analytics
# ---------------------------------------------------------------------------

def _client_df(query: str, params=()):
    user = _get_current_user()
    db_path = _get_client_db(user["id"])
    return _df(query, params, db_path=db_path)

@app.route("/api/client/kpis")
@client_required
def api_client_kpis():
    try:
        df = _client_df("""
            SELECT COUNT(*) AS total_orders, SUM(total_amount) AS total_revenue,
                   AVG(total_amount) AS avg_order_value, SUM(quantity) AS total_units_sold,
                   COUNT(DISTINCT customer_id) AS unique_customers,
                   COUNT(DISTINCT product_id) AS unique_products
            FROM fact_sales
        """)
        row = df.iloc[0].to_dict()
        for key in ("total_revenue", "avg_order_value"):
            if row.get(key) is not None:
                row[key] = round(float(row[key]), 2)
        return jsonify(row)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/sales/timeseries")
@client_required
def api_client_sales_timeseries():
    try:
        granularity = request.args.get("granularity", "monthly")
        if granularity == "daily":
            df = _client_df("SELECT date_id AS period, SUM(total_amount) AS revenue, SUM(quantity) AS units FROM fact_sales GROUP BY date_id ORDER BY date_id")
        elif granularity == "weekly":
            df = _client_df("""
                SELECT d.year || '-W' || printf('%02d', d.week) AS period,
                       SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
                FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.year, d.week ORDER BY d.year, d.week
            """)
        else:
            df = _client_df("""
                SELECT d.year || '-' || printf('%02d', d.month) AS period,
                       SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
                FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.year, d.month ORDER BY d.year, d.month
            """)
        df["revenue"] = df["revenue"].round(2)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/products/top")
@client_required
def api_client_top_products():
    try:
        limit = min(int(request.args.get("limit", 10)), 50)
        df = _client_df("""
            SELECT p.product_id, p.name, p.category,
                   SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units_sold, COUNT(*) AS order_count
            FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id
            GROUP BY p.product_id ORDER BY revenue DESC LIMIT ?
        """, (limit,))
        df["revenue"] = df["revenue"].round(2)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/sales/region")
@client_required
def api_client_sales_region():
    try:
        df = _client_df("""
            SELECT region, SUM(total_amount) AS revenue, SUM(quantity) AS units_sold,
                   COUNT(*) AS order_count, COUNT(DISTINCT customer_id) AS customers
            FROM fact_sales GROUP BY region ORDER BY revenue DESC
        """)
        df["revenue"] = df["revenue"].round(2)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/sales/category")
@client_required
def api_client_sales_category():
    try:
        df = _client_df("""
            SELECT p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units_sold
            FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id
            GROUP BY p.category ORDER BY revenue DESC
        """)
        df["revenue"] = df["revenue"].round(2)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/customers")
@client_required
def api_client_customers():
    try:
        page = max(1, int(request.args.get("page", 1)))
        per_page = min(int(request.args.get("per_page", 20)), 100)
        offset = (page - 1) * per_page
        df = _client_df("""
            SELECT c.customer_id, c.name, c.region, c.country,
                   COUNT(f.sale_id) AS order_count, SUM(f.total_amount) AS lifetime_value
            FROM dim_customers c LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
            GROUP BY c.customer_id ORDER BY lifetime_value DESC LIMIT ? OFFSET ?
        """, (per_page, offset))
        if "lifetime_value" in df.columns:
            df["lifetime_value"] = df["lifetime_value"].round(2)
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Client API: forecast
# ---------------------------------------------------------------------------

@app.route("/api/client/forecast")
@client_required
def api_client_forecast():
    try:
        import json as _json
        from models.ensemble_forecasting import ensemble_forecast
        from models.forecasting import persist_forecast
        from models.data_quality import score_from_db
        horizon = min(int(request.args.get("horizon", 30)), 90)
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        result = ensemble_forecast(horizon=horizon, db_path=db_path)

        dq_score = score_from_db(db_path)

        # Persist the forecast run + values
        try:
            run_meta = {
                "algorithm":          result.get("algorithm", "Ensemble"),
                "horizon":            horizon,
                "mae":                result.get("mae"),
                "rmse":               result.get("rmse"),
                "mape":               result.get("mape"),
                "training_start":     result.get("training_start"),
                "training_end":       result.get("training_end"),
                "training_days":      result.get("training_days"),
                "weights_json":       _json.dumps(result.get("weights", {})),
                "error_msg":          "; ".join(result.get("errors", [])) or None,
                "data_quality_score": dq_score,
            }
            persist_forecast(db_path, run_meta, result["forecast"])
        except Exception as persist_err:
            app.logger.warning("Forecast persistence failed: %s", persist_err)

        return jsonify(result)
    except Exception as e:
        app.logger.exception("Forecast endpoint error")
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/forecast/summary")
@client_required
def api_client_forecast_summary():
    try:
        from models.prophet_forecasting import get_forecast_summary
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        summary = get_forecast_summary(db_path=db_path)
        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/forecast/accuracy")
@client_required
def api_client_forecast_accuracy():
    try:
        from models.ensemble_forecasting import ensemble_forecast
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        result = ensemble_forecast(horizon=7, db_path=db_path)
        return jsonify({
            "status":    "ok",
            "algorithm": result.get("algorithm", "Ensemble"),
            "mae":       result.get("mae"),
            "rmse":      result.get("rmse"),
            "mape":      result.get("mape"),
            "weights":   result.get("weights", {}),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/forecast/anomalies")
@client_required
def api_client_forecast_anomalies():
    try:
        from models.anomaly import run_anomaly_detection
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        result = run_anomaly_detection(db_path)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/forecast/whatif", methods=["POST"])
@client_required
def api_client_forecast_whatif():
    try:
        from models.ensemble_forecasting import ensemble_forecast
        from models.whatif import run_whatif
        data = request.get_json(silent=True) or {}
        scenario = data.get("scenario", {})
        horizon  = min(int(data.get("horizon", 30)), 90)
        user     = _get_current_user()
        db_path  = _get_client_db(user["id"])
        base     = ensemble_forecast(horizon=horizon, db_path=db_path)
        corridor = run_whatif(base["forecast"], scenario)
        return jsonify({
            "scenario":  scenario,
            "horizon":   horizon,
            "corridor":  corridor,
        })
    except Exception as e:
        app.logger.exception("What-if endpoint error")
        return jsonify({"error": str(e)}), 500

@app.route("/api/client/insights")
@client_required
def api_client_insights():
    try:
        from models.ensemble_forecasting import ensemble_forecast
        from models.anomaly import run_anomaly_detection
        from models.data_quality import score_from_db
        user    = _get_current_user()
        db_path = _get_client_db(user["id"])

        forecast_result = ensemble_forecast(horizon=30, db_path=db_path)
        anomaly_result  = run_anomaly_detection(db_path)
        dq_score        = score_from_db(db_path)

        insights = _generate_insights(forecast_result, anomaly_result, dq_score, db_path)
        return jsonify({"insights": insights, "data_quality_score": dq_score})
    except Exception as e:
        app.logger.exception("Insights endpoint error")
        return jsonify({"error": str(e)}), 500


def _generate_insights(forecast_result: dict, anomaly_result: dict, dq_score: float, db_path: str) -> list:
    """Generate plain-language insights from forecast, anomaly, and KPI data."""
    insights = []
    confidence = "high" if dq_score >= 70 else ("medium" if dq_score >= 40 else "low")

    # --- Forecast trend insight ---
    forecast = forecast_result.get("forecast", [])
    if len(forecast) >= 14:
        first_week  = sum(d["yhat"] for d in forecast[:7])
        second_week = sum(d["yhat"] for d in forecast[7:14])
        if first_week > 0:
            pct = (second_week - first_week) / first_week * 100
            direction = "increase" if pct > 0 else "decrease"
            if abs(pct) > 2:
                insights.append({
                    "type":       "forecast_trend",
                    "confidence": confidence,
                    "message":    (
                        f"Revenue is forecast to {direction} by {abs(pct):.1f}% "
                        f"in week 2 vs week 1 of the forecast period."
                    ),
                })

    # --- Anomaly insight ---
    anomalies = anomaly_result.get("anomalies", [])
    if anomalies:
        recent = sorted(anomalies, key=lambda x: x.get("date", ""), reverse=True)[:3]
        for a in recent:
            insights.append({
                "type":       "anomaly",
                "confidence": confidence,
                "message":    f"Anomaly detected on {a['date']}: {a['description']}",
            })

    # --- Accuracy insight ---
    mae = forecast_result.get("mae")
    if mae is not None:
        insights.append({
            "type":       "accuracy",
            "confidence": confidence,
            "message":    (
                f"Ensemble forecast model achieved MAE={mae:.2f} on holdout data. "
                f"Component weights: "
                + ", ".join(
                    f"{k}={v:.0%}"
                    for k, v in forecast_result.get("weights", {}).items()
                    if v > 0
                ) + "."
            ),
        })

    # --- Data quality insight ---
    if dq_score < 70:
        insights.append({
            "type":       "data_quality",
            "confidence": "low",
            "message":    (
                f"Data quality score is {dq_score:.0f}/100. "
                "Forecast confidence may be reduced. "
                "Consider uploading cleaner or more complete data."
            ),
        })
    else:
        insights.append({
            "type":       "data_quality",
            "confidence": "high",
            "message":    f"Data quality score: {dq_score:.0f}/100 — good quality data.",
        })

    # --- KPI-based insight ---
    try:
        conn = _get_db(db_path)
        row = conn.execute(
            "SELECT SUM(total_amount) AS rev, COUNT(DISTINCT date_id) AS days "
            "FROM fact_sales"
        ).fetchone()
        conn.close()
        if row and row["days"] and row["rev"]:
            avg_daily = row["rev"] / row["days"]
            insights.append({
                "type":       "kpi",
                "confidence": confidence,
                "message":    (
                    f"Average daily revenue across {row['days']} trading days: "
                    f"${avg_daily:,.2f}."
                ),
            })
    except Exception:
        pass

    return insights

# ---------------------------------------------------------------------------
# Client API: reports (CSV export)
# ---------------------------------------------------------------------------

@app.route("/api/client/reports/sales.csv")
@client_required
def api_client_report_sales_csv():
    try:
        import io
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        df = _df("""
            SELECT f.sale_id, f.date_id, p.name AS product, p.category,
                   c.name AS customer, c.region, f.quantity, f.unit_price, f.total_amount
            FROM fact_sales f
            JOIN dim_products p ON f.product_id = p.product_id
            JOIN dim_customers c ON f.customer_id = c.customer_id
            ORDER BY f.date_id
        """, db_path=db_path)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, mimetype="text/csv", as_attachment=True,
                         download_name=f"sales_report_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Current-user info endpoint (used by frontend to detect role)
# ---------------------------------------------------------------------------

@app.route("/api/me")
def api_me():
    """GET /api/me – returns the current user's role and auth status."""
    user = _get_current_user()
    if user:
        return jsonify({"authenticated": True, "role": user["role"]})
    return jsonify({"authenticated": False, "role": None})

# ---------------------------------------------------------------------------
# Legacy / public analytics APIs (kept for backward compat with index.html)
# ---------------------------------------------------------------------------

@app.route("/api/kpis")
def kpis():
    df = _df("""
        SELECT COUNT(*) AS total_orders, SUM(total_amount) AS total_revenue,
               AVG(total_amount) AS avg_order_value, SUM(quantity) AS total_units_sold,
               COUNT(DISTINCT customer_id) AS unique_customers,
               COUNT(DISTINCT product_id) AS unique_products
        FROM fact_sales
    """)
    row = df.iloc[0].to_dict()
    for key in ("total_revenue", "avg_order_value"):
        if row[key] is not None:
            row[key] = round(float(row[key]), 2)
    return jsonify(row)

@app.route("/api/sales/timeseries")
def sales_timeseries():
    granularity = request.args.get("granularity", "monthly")
    if granularity == "daily":
        df = _df("SELECT date_id AS period, SUM(total_amount) AS revenue, SUM(quantity) AS units FROM fact_sales GROUP BY date_id ORDER BY date_id")
    elif granularity == "weekly":
        df = _df("""
            SELECT d.year || '-W' || printf('%02d', d.week) AS period,
                   SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
            FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.week ORDER BY d.year, d.week
        """)
    else:
        df = _df("""
            SELECT d.year || '-' || printf('%02d', d.month) AS period,
                   SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
            FROM fact_sales f JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.month ORDER BY d.year, d.month
        """)
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/products/top")
def top_products():
    limit = min(int(request.args.get("limit", 10)), 50)
    df = _df("""
        SELECT p.product_id, p.name, p.category,
               SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units_sold, COUNT(*) AS order_count
        FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.product_id ORDER BY revenue DESC LIMIT ?
    """, (limit,))
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/sales/region")
def sales_by_region():
    df = _df("""
        SELECT region, SUM(total_amount) AS revenue, SUM(quantity) AS units_sold,
               COUNT(*) AS order_count, COUNT(DISTINCT customer_id) AS customers
        FROM fact_sales GROUP BY region ORDER BY revenue DESC
    """)
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/sales/category")
def sales_by_category():
    df = _df("""
        SELECT p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units_sold
        FROM fact_sales f JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.category ORDER BY revenue DESC
    """)
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/forecast")
def forecast():
    from models.forecasting import forecast_sales
    horizon = min(int(request.args.get("horizon", 30)), 90)
    db_path = app.config.get("DATABASE", DB_PATH)
    results = forecast_sales(horizon=horizon, db_path=db_path)
    return jsonify(results)

@app.route("/api/forecast/summary")
def forecast_summary():
    from models.forecasting import get_model_summary
    db_path = app.config.get("DATABASE", DB_PATH)
    summary = get_model_summary(db_path=db_path)
    return jsonify(summary)

@app.route("/api/customers")
def customers():
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    offset = (page - 1) * per_page
    df = _df("""
        SELECT c.customer_id, c.name, c.region, c.country,
               COUNT(f.sale_id) AS order_count, SUM(f.total_amount) AS lifetime_value
        FROM dim_customers c LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
        GROUP BY c.customer_id ORDER BY lifetime_value DESC LIMIT ? OFFSET ?
    """, (per_page, offset))
    if "lifetime_value" in df.columns:
        df["lifetime_value"] = df["lifetime_value"].round(2)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/etl/run", methods=["POST"])
def run_etl():
    try:
        from etl.load import run_etl as _run_etl
        db_path = app.config.get("DATABASE", DB_PATH)
        counts = _run_etl(db_path=db_path)
        return jsonify({"status": "success", "rows_loaded": counts})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500

@app.route("/api/admin/predict/upload", methods=["POST"])
def predict_upload():
    from models.forecasting import forecast_from_dataframe
    if "file" not in request.files:
        return jsonify({"error": "No file provided."}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected."}), 400
    filename = secure_filename(file.filename)
    if not filename:
        return jsonify({"error": "Invalid filename."}), 400
    if not filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are accepted."}), 400
    file.seek(0, 2)
    size = file.tell()
    file.seek(0)
    if size > _MAX_UPLOAD_BYTES:
        return jsonify({"error": f"File too large (max {_MAX_UPLOAD_BYTES // (1024 * 1024)} MB)."}), 400
    try:
        horizon = min(int(request.form.get("horizon", 30)), 90)
    except (TypeError, ValueError):
        horizon = 30
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    save_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex[:8]}_{filename}")
    file.save(save_path)
    try:
        df = pd.read_csv(save_path)
        result = forecast_from_dataframe(df, horizon=horizon)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 422
    except Exception as exc:
        return jsonify({"error": f"Processing error: {str(exc)}"}), 500
    finally:
        try:
            os.remove(save_path)
        except OSError:
            pass

# ---------------------------------------------------------------------------
# Required API endpoints (route aliases + new endpoints)
# ---------------------------------------------------------------------------

@app.route("/api/upload", methods=["POST"])
@client_required
def api_upload():
    """POST /api/upload – upload a CSV/XLSX sales file (client authenticated)."""
    return api_client_upload()


@app.route("/api/sales/trend")
def api_sales_trend():
    """GET /api/sales/trend – alias for /api/sales/timeseries."""
    return sales_timeseries()


@app.route("/api/top-products")
def api_top_products():
    """GET /api/top-products – alias for /api/products/top."""
    return top_products()


@app.route("/api/regions")
def api_regions():
    """GET /api/regions – alias for /api/sales/region."""
    return sales_by_region()


# ---------------------------------------------------------------------------
# Export endpoints
# ---------------------------------------------------------------------------

@app.route("/api/export/sales.csv")
@client_required
def api_export_sales_csv():
    """GET /api/export/sales.csv – download full sales data for authenticated client."""
    try:
        import io
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        df = _df("""
            SELECT f.sale_id, f.date_id, p.name AS product, p.category,
                   c.name AS customer, c.region, f.quantity, f.unit_price, f.total_amount
            FROM fact_sales f
            JOIN dim_products p ON f.product_id = p.product_id
            JOIN dim_customers c ON f.customer_id = c.customer_id
            ORDER BY f.date_id
        """, db_path=db_path)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, mimetype="text/csv", as_attachment=True,
                         download_name=f"sales_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/forecast.csv")
@client_required
def api_export_forecast_csv():
    """GET /api/export/forecast.csv – download the latest persisted forecast for authenticated client."""
    try:
        import io
        from models.forecasting import get_latest_forecast_run
        user = _get_current_user()
        db_path = _get_client_db(user["id"])
        run = get_latest_forecast_run(db_path)
        if not run or not run.get("values"):
            return jsonify({"error": "No forecast data available. Run a forecast first."}), 404
        rows = [{"date": v["ds"], "forecast": v["yhat"],
                 "lower": v["yhat_lower"], "upper": v["yhat_upper"]}
                for v in run["values"]]
        df = pd.DataFrame(rows)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, mimetype="text/csv", as_attachment=True,
                         download_name=f"forecast_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=False, port=5000)
