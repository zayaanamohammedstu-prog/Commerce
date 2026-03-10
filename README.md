# Commerce

# Meridian · Commerce Analytics

A full-stack commerce analytics platform with an ETL pipeline, SQLite data warehouse, REST API, and interactive dashboard.

---

## Running the app

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start the server (runs ETL then launches Flask on port 5000)
python run.py
```

Open **http://localhost:5000** in your browser.

---

## Project structure

```
api/           Flask REST API
data/raw/      Source CSVs (products, customers, sales)
etl/           Extract → Transform → Load pipeline
frontend/      HTML template + CSS + JS (Chart.js dashboard)
models/        Sales forecasting (Holt-Winters)
tests/         pytest test suite
uploads/       Uploaded CSVs for ad-hoc prediction (auto-created)
warehouse/     SQLite schema & connection helpers
run.py         Entry point
```

---

## Admin section

The **Admin** tab (section 06 in the sidebar) exposes two tools:

### 1 · Run ETL Sync

Click **Run ETL Sync** to re-run the full Extract → Transform → Load pipeline
against `data/raw/`.  The dashboard refreshes automatically on success.

### 2 · Predict from CSV (file upload)

Upload any CSV file that contains at least a **`date`** column plus one of:

| Column(s)                 | Description                        |
|---------------------------|------------------------------------|
| `revenue`                 | Pre-computed daily revenue         |
| `total_amount`            | Alias for `revenue`                |
| `quantity` + `unit_price` | Revenue computed as qty × price    |

**Example CSV:**

```csv
date,revenue
2024-01-01,1200.50
2024-01-02,980.00
2024-01-03,1345.75
...
```

Select a forecast horizon (14 / 30 / 60 / 90 days) then click
**Generate Forecast**.  Results appear as a chart and a scrollable table.

The endpoint used is `POST /api/admin/predict/upload`
(`multipart/form-data`, field name `file`, optional `horizon` field).

---

## API reference (brief)

| Method | Endpoint                       | Purpose                         |
|--------|--------------------------------|---------------------------------|
| GET    | `/api/kpis`                    | KPI summary                     |
| GET    | `/api/sales/timeseries`        | Time-series revenue/units       |
| GET    | `/api/products/top`            | Top products by revenue         |
| GET    | `/api/sales/region`            | Revenue by region               |
| GET    | `/api/sales/category`          | Revenue by category             |
| GET    | `/api/forecast`                | Holt-Winters forecast           |
| GET    | `/api/forecast/summary`        | Model metadata                  |
| GET    | `/api/customers`               | Paginated customer list         |
| POST   | `/api/etl/run`                 | Trigger ETL pipeline            |
| POST   | `/api/admin/predict/upload`    | Upload CSV for ad-hoc forecast  |

---

## Running tests

```bash
pytest tests/ -v
```
