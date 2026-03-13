# ShopIntel

**ShopIntel** is a secure, full-stack Business Intelligence and sales forecasting web application for retail businesses. It lets you evaluate past sales records, generate time-series forecasts, and explore interactive dashboards — all from a clean, multi-tenant platform.

---

## Features

- **Automated ETL** – ingest CSV and Excel sales files, clean and transform data with Pandas, load into a SQLite star-schema warehouse via SQLAlchemy
- **Time-series forecasting** – Prophet-powered daily revenue forecasts with MAE/RMSE/MAPE accuracy metrics; falls back to Holt-Winters or moving-average when Prophet is unavailable
- **Interactive BI dashboard** – Chart.js charts for KPIs, revenue trends, top products, regional breakdown, and category analysis
- **Downloadable reports** – export sales and forecast data as CSV
- **Secure multi-tenant architecture** – per-client SQLite warehouse, session-based auth, role guards (admin/client), inactivity logout, and file-upload validation

---

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) set environment variables
export SECRET_KEY="your-secret-key-here"

# 3. Start the server (runs seed ETL then launches Flask on port 5000)
python run.py
```

Open **http://localhost:5000** in your browser.

Default admin credentials (created on first run):
- Email: `admin@commerce.local`
- Password: `Admin1234!`

---

## Environment variables

| Variable          | Default                                | Description                                 |
|-------------------|----------------------------------------|---------------------------------------------|
| `SECRET_KEY`      | `commerce-dev-secret-key-…`            | Flask session secret — **change in prod**   |
| `DATABASE`        | `data/commerce.db`                     | Path to the default SQLite warehouse        |
| `PLATFORM_DATABASE` | `data/platform.db`                    | Path to the platform (auth) SQLite DB       |

---

## Project structure

```
api/              Flask application (routes, auth, upload, API)
data/
  raw/            Seed CSV files (products.csv, customers.csv, sales.csv)
  clients/        Per-client warehouse DBs (auto-created on approval)
etl/              Extract → Transform → Load pipeline
  extract.py      Read raw CSVs
  transform.py    Clean and normalise DataFrames
  load.py         Upsert into warehouse tables
  client_pipeline.py  ETL for uploaded CSV/Excel files (uses SQLAlchemy)
frontend/
  templates/      Jinja2 HTML templates
  static/
    js/app.js     Chart.js dashboard logic
    css/style.css Styles
models/
  forecasting.py        Holt-Winters + fallback; persist_forecast helper
  prophet_forecasting.py  Prophet-based forecasting with accuracy metrics
tests/            pytest test suite
warehouse/
  schema.sql      Star schema + forecast/upload tables
  platform.sql    Auth/user schema
  database.py     SQLite connection & schema init helpers
  platform_db.py  Platform DB helpers
run.py            Entry point: seed ETL + start Flask
```

---

## How to run ETL

**Seed ETL (raw CSV files → default warehouse):**

```bash
# Via API
curl -X POST http://localhost:5000/api/etl/run

# Or via Python
python -c "from etl.load import run_etl; print(run_etl())"
```

**Client upload (CSV or Excel via browser):** log in as an approved client, go to **Upload** in the sidebar, and select your file.

### Sample CSV format

The upload endpoint accepts flexible column names. At minimum your file needs a **date** column and a **revenue** column (or an alias):

```csv
date,revenue
2024-01-01,1200.50
2024-01-02,980.00
2024-01-03,1345.75
```

Accepted revenue column aliases: `revenue`, `total_amount`, `amount`, `sales`, or `quantity` + `unit_price`.

Accepted date column aliases: `date`, `order_date`, `sale_date`, `transaction_date`, `purchase_date`.

Full sales file example (with dimension columns):

```csv
sale_id,date,product_id,product_name,category,customer_id,customer_name,region,country,quantity,unit_price,total_amount
S001,2024-01-15,P001,Laptop,Electronics,C001,Alice,North America,USA,2,1200.00,2400.00
S002,2024-01-16,P003,Desk,Furniture,C002,Bob,Europe,UK,1,499.99,499.99
```

---

## How to generate forecasts

**Via browser:** log in as a client and visit **Forecast** in the sidebar.

**Via API:**

```bash
# 30-day forecast
curl http://localhost:5000/api/client/forecast?horizon=30

# Accuracy metrics
curl http://localhost:5000/api/client/forecast/accuracy

# Forecast summary / model metadata
curl http://localhost:5000/api/client/forecast/summary
```

Forecasts are automatically persisted to the `forecast_runs` and `forecast_values` warehouse tables.

---

## API reference

### Public / legacy (no auth required)

| Method | Endpoint                       | Description                         |
|--------|--------------------------------|-------------------------------------|
| GET    | `/api/kpis`                    | KPI summary (all data)              |
| GET    | `/api/sales/timeseries`        | Revenue/units over time             |
| GET    | `/api/sales/trend`             | Alias for `/api/sales/timeseries`   |
| GET    | `/api/products/top`            | Top products by revenue             |
| GET    | `/api/top-products`            | Alias for `/api/products/top`       |
| GET    | `/api/sales/region`            | Revenue by region                   |
| GET    | `/api/regions`                 | Alias for `/api/sales/region`       |
| GET    | `/api/sales/category`          | Revenue by category                 |
| GET    | `/api/forecast`                | Holt-Winters forecast               |
| GET    | `/api/forecast/summary`        | Model metadata                      |
| GET    | `/api/customers`               | Paginated customer list             |
| POST   | `/api/etl/run`                 | Trigger seed ETL pipeline           |
| POST   | `/api/admin/predict/upload`    | Upload CSV for ad-hoc forecast      |

### Client (requires login + approved status)

| Method | Endpoint                          | Description                              |
|--------|-----------------------------------|------------------------------------------|
| POST   | `/api/upload`                     | Upload CSV/XLSX sales file               |
| POST   | `/api/client/upload`              | Same as above                            |
| GET    | `/api/client/kpis`                | KPIs for authenticated client            |
| GET    | `/api/client/sales/timeseries`    | Time-series for authenticated client     |
| GET    | `/api/client/products/top`        | Top products for authenticated client    |
| GET    | `/api/client/sales/region`        | Region breakdown for authenticated client|
| GET    | `/api/client/sales/category`      | Category breakdown                       |
| GET    | `/api/client/customers`           | Customer list for authenticated client   |
| GET    | `/api/client/forecast`            | Forecast + persists run                  |
| GET    | `/api/client/forecast/summary`    | Forecast model summary                   |
| GET    | `/api/client/forecast/accuracy`   | Accuracy metrics (MAE/RMSE/MAPE)        |
| GET    | `/api/export/sales.csv`           | Download sales CSV                       |
| GET    | `/api/export/forecast.csv`        | Download latest forecast CSV             |
| GET    | `/api/client/reports/sales.csv`   | Same as `/api/export/sales.csv`          |

### Admin (requires admin role)

| Method | Endpoint                               | Description                 |
|--------|----------------------------------------|-----------------------------|
| GET    | `/api/admin/users`                     | List all users              |
| GET    | `/api/admin/users/pending`             | List pending users          |
| POST   | `/api/admin/users/create`              | Create user                 |
| POST   | `/api/admin/users/<id>/approve`        | Approve client              |
| POST   | `/api/admin/users/<id>/disable`        | Disable client              |
| POST   | `/api/admin/users/<id>/enable`         | Enable client               |
| POST   | `/api/admin/users/<id>/reset-password` | Reset user password         |

---

## Running tests

```bash
pytest tests/ -v
```

---

## Warehouse schema

The SQLite warehouse uses a **star schema**:

| Table             | Purpose                                         |
|-------------------|-------------------------------------------------|
| `dim_products`    | Product dimension (id, name, category, price)   |
| `dim_customers`   | Customer dimension (id, name, email, region)    |
| `dim_date`        | Date dimension (year, quarter, month, week, day)|
| `fact_sales`      | Central fact table (quantity, revenue, FK refs) |
| `forecast_runs`   | Forecast run metadata (algorithm, metrics)      |
| `forecast_values` | Forecasted values per run (date, yhat)          |
| `upload_log`      | Upload/ETL audit trail                          |

See `warehouse/schema.sql` for the full DDL with indexes and constraints.

