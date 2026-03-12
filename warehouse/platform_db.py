"""
warehouse/platform_db.py
Handles SQLite connection and schema init for the platform (auth/users) database.
"""
import sqlite3
import os

PLATFORM_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "platform.db")
PLATFORM_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "platform.sql")
CLIENTS_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clients")


def get_platform_connection(db_path: str = None) -> sqlite3.Connection:
    path = db_path or PLATFORM_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_platform_db(db_path: str = None) -> None:
    path = db_path or PLATFORM_DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(PLATFORM_SCHEMA_PATH, "r") as f:
        schema = f.read()
    conn = get_platform_connection(path)
    conn.executescript(schema)
    conn.commit()
    conn.close()


def get_client_db_path(user_id: int) -> str:
    os.makedirs(CLIENTS_DIR, exist_ok=True)
    return os.path.join(CLIENTS_DIR, f"client_{user_id}.db")


def create_client_db(user_id: int) -> str:
    """Initialize a new warehouse DB for a client and return the path."""
    from warehouse.database import init_db
    db_path = get_client_db_path(user_id)
    init_db(db_path)
    return db_path
