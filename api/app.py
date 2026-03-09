"""
api/app.py
Flask REST API that serves KPI data, BI analytics, and sales forecasts.
"""
import os
import sys

# Ensure repo root is on the path so warehouse/etl/models can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import Flask, jsonify, request, render_template, abort
from flask_cors import CORS
import pandas as pd

from warehouse.database import get_connection, DB_PATH
from models.forecasting import forecast_sales, get_model_summary

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "templates")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "static")

app = Flask(
    __name__,
    template_folder=TEMPLATE_DIR,
    static_folder=STATIC_DIR,
)
CORS(app)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_db() -> "sqlite3.Connection":
    db_path = app.config.get("DATABASE", DB_PATH)
    return get_connection(db_path)


def _df(query: str, params=()) -> pd.DataFrame:
    conn = _get_db()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Web routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# ETL trigger
# ---------------------------------------------------------------------------

@app.route("/api/etl/run", methods=["POST"])
def run_etl():
    try:
        from etl.load import run_etl as _run_etl
        db_path = app.config.get("DATABASE", DB_PATH)
        counts = _run_etl(db_path=db_path)
        return jsonify({"status": "success", "rows_loaded": counts})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


# ---------------------------------------------------------------------------
# KPI summary
# ---------------------------------------------------------------------------

@app.route("/api/kpis")
def kpis():
    df = _df("""
        SELECT
            COUNT(*)                           AS total_orders,
            SUM(total_amount)                  AS total_revenue,
            AVG(total_amount)                  AS avg_order_value,
            SUM(quantity)                      AS total_units_sold,
            COUNT(DISTINCT customer_id)        AS unique_customers,
            COUNT(DISTINCT product_id)         AS unique_products
        FROM fact_sales
    """)
    row = df.iloc[0].to_dict()
    # round floats
    for key in ("total_revenue", "avg_order_value"):
        if row[key] is not None:
            row[key] = round(float(row[key]), 2)
    return jsonify(row)


# ---------------------------------------------------------------------------
# Sales time series
# ---------------------------------------------------------------------------

@app.route("/api/sales/timeseries")
def sales_timeseries():
    granularity = request.args.get("granularity", "monthly")

    if granularity == "daily":
        df = _df("""
            SELECT date_id AS period, SUM(total_amount) AS revenue, SUM(quantity) AS units
            FROM fact_sales
            GROUP BY date_id
            ORDER BY date_id
        """)
    elif granularity == "weekly":
        df = _df("""
            SELECT
                d.year || '-W' || printf('%02d', d.week) AS period,
                SUM(f.total_amount) AS revenue,
                SUM(f.quantity)     AS units
            FROM fact_sales f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.week
            ORDER BY d.year, d.week
        """)
    else:  # monthly (default)
        df = _df("""
            SELECT
                d.year || '-' || printf('%02d', d.month) AS period,
                SUM(f.total_amount) AS revenue,
                SUM(f.quantity)     AS units
            FROM fact_sales f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.year, d.month
            ORDER BY d.year, d.month
        """)

    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Top-performing products
# ---------------------------------------------------------------------------

@app.route("/api/products/top")
def top_products():
    limit = min(int(request.args.get("limit", 10)), 50)
    df = _df("""
        SELECT
            p.product_id,
            p.name,
            p.category,
            SUM(f.total_amount) AS revenue,
            SUM(f.quantity)     AS units_sold,
            COUNT(*)            AS order_count
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.product_id
        ORDER BY revenue DESC
        LIMIT ?
    """, (limit,))
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Sales by region
# ---------------------------------------------------------------------------

@app.route("/api/sales/region")
def sales_by_region():
    df = _df("""
        SELECT
            region,
            SUM(total_amount) AS revenue,
            SUM(quantity)     AS units_sold,
            COUNT(*)          AS order_count,
            COUNT(DISTINCT customer_id) AS customers
        FROM fact_sales
        GROUP BY region
        ORDER BY revenue DESC
    """)
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Sales by category
# ---------------------------------------------------------------------------

@app.route("/api/sales/category")
def sales_by_category():
    df = _df("""
        SELECT
            p.category,
            SUM(f.total_amount) AS revenue,
            SUM(f.quantity)     AS units_sold
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.category
        ORDER BY revenue DESC
    """)
    df["revenue"] = df["revenue"].round(2)
    return jsonify(df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

@app.route("/api/forecast")
def forecast():
    horizon = min(int(request.args.get("horizon", 30)), 90)
    db_path = app.config.get("DATABASE", DB_PATH)
    results = forecast_sales(horizon=horizon, db_path=db_path)
    return jsonify(results)


@app.route("/api/forecast/summary")
def forecast_summary():
    db_path = app.config.get("DATABASE", DB_PATH)
    summary = get_model_summary(db_path=db_path)
    return jsonify(summary)


# ---------------------------------------------------------------------------
# Customer list (paginated)
# ---------------------------------------------------------------------------

@app.route("/api/customers")
def customers():
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    offset = (page - 1) * per_page
    df = _df("""
        SELECT c.customer_id, c.name, c.region, c.country,
               COUNT(f.sale_id)       AS order_count,
               SUM(f.total_amount)    AS lifetime_value
        FROM dim_customers c
        LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
        GROUP BY c.customer_id
        ORDER BY lifetime_value DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset))
    if "lifetime_value" in df.columns:
        df["lifetime_value"] = df["lifetime_value"].round(2)
    return jsonify(df.to_dict(orient="records"))


if __name__ == "__main__":
    app.run(debug=False, port=5000)
