"""
warehouse/database.py
Handles SQLite connection and schema initialisation for the data warehouse.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "commerce.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a SQLite connection with row-factory set to Row."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    """Create tables from schema.sql if they do not already exist."""
    with open(SCHEMA_PATH, "r") as f:
        schema = f.read()
    conn = get_connection(db_path)
    conn.executescript(schema)
    conn.commit()
    _migrate(conn)
    conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply safe ALTER TABLE migrations for existing databases."""
    # Extend forecast_runs with new columns (guarded – no-op if they exist)
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(forecast_runs)").fetchall()
    }
    new_cols = {
        "weights_json": "TEXT",
        "error_msg": "TEXT",
        "data_quality_score": "REAL",
    }
    for col, col_type in new_cols.items():
        if col not in existing:
            conn.execute(
                f"ALTER TABLE forecast_runs ADD COLUMN {col} {col_type}"
            )
    # Create forecast_events if it somehow doesn't exist yet
    conn.execute(
        """CREATE TABLE IF NOT EXISTS forecast_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            type        TEXT NOT NULL,
            severity    REAL,
            description TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fe_date ON forecast_events(date)"
    )
    conn.commit()


def drop_all(db_path: str = DB_PATH) -> None:
    """Drop all warehouse tables (used in tests / full reloads)."""
    tables = [
        "forecast_values", "forecast_runs", "forecast_events",
        "upload_log", "fact_sales", "dim_date", "dim_customers", "dim_products",
    ]
    conn = get_connection(db_path)
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    conn.close()
