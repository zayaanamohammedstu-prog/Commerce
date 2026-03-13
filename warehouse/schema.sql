-- =============================================================
-- Data Warehouse Schema for Commerce Analytics
-- Star-schema design: fact table + dimension tables
-- =============================================================

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- Dimension: Products
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    product_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    category     TEXT NOT NULL,
    unit_price   REAL NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- Dimension: Customers
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id  TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    email        TEXT,
    region       TEXT NOT NULL,
    country      TEXT NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- Dimension: Date
-- Populated by ETL pipeline for every date in the data range
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id   TEXT PRIMARY KEY,  -- ISO format: YYYY-MM-DD
    year      INTEGER NOT NULL,
    quarter   INTEGER NOT NULL,
    month     INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week      INTEGER NOT NULL,
    day       INTEGER NOT NULL,
    day_name  TEXT NOT NULL,
    is_weekend INTEGER NOT NULL DEFAULT 0
);

-- ------------------------------------------------------------
-- Fact: Sales
-- Central fact table with foreign keys to all dimensions
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id      TEXT PRIMARY KEY,
    date_id      TEXT NOT NULL REFERENCES dim_date(date_id),
    product_id   TEXT NOT NULL REFERENCES dim_products(product_id),
    customer_id  TEXT NOT NULL REFERENCES dim_customers(customer_id),
    quantity     INTEGER NOT NULL,
    unit_price   REAL NOT NULL,
    total_amount REAL NOT NULL,
    region       TEXT NOT NULL,
    loaded_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- Indexes for common query patterns
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sales_date       ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_sales_product    ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_customer   ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_region     ON fact_sales(region);
CREATE INDEX IF NOT EXISTS idx_date_year_month  ON dim_date(year, month);

-- ------------------------------------------------------------
-- Forecast runs metadata
-- One row per forecast run (model, parameters, accuracy).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS forecast_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    algorithm    TEXT NOT NULL,       -- e.g. "Prophet", "Holt-Winters", "Moving Average"
    horizon      INTEGER NOT NULL,    -- number of days forecast
    mae          REAL,
    rmse         REAL,
    mape         REAL,
    training_start TEXT,
    training_end   TEXT,
    training_days  INTEGER,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------------------
-- Forecast values
-- One row per forecasted date per run.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS forecast_values (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id   INTEGER NOT NULL REFERENCES forecast_runs(id),
    ds       TEXT NOT NULL,   -- ISO date YYYY-MM-DD
    yhat     REAL NOT NULL,
    yhat_lower REAL,
    yhat_upper REAL
);

CREATE INDEX IF NOT EXISTS idx_fv_run_id ON forecast_values(run_id);
CREATE INDEX IF NOT EXISTS idx_fv_ds     ON forecast_values(ds);

-- ------------------------------------------------------------
-- Upload log
-- Records every file upload and ETL run for audit purposes.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS upload_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER,      -- references app_users.id (platform DB)
    filename     TEXT NOT NULL,
    file_size    INTEGER,
    rows_accepted INTEGER,
    rows_rejected INTEGER,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending / success / error
    error_msg    TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
