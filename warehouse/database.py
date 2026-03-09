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
    conn.close()


def drop_all(db_path: str = DB_PATH) -> None:
    """Drop all warehouse tables (used in tests / full reloads)."""
    tables = ["fact_sales", "dim_date", "dim_customers", "dim_products"]
    conn = get_connection(db_path)
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    conn.close()
