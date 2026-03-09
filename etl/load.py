"""
etl/load.py
Load transformed DataFrames into the SQLite data warehouse.
"""
import pandas as pd
from warehouse.database import get_connection, init_db, DB_PATH


def _upsert_df(df: pd.DataFrame, table: str, pk: str, db_path: str) -> int:
    """Insert rows, skipping duplicates based on primary key."""
    conn = get_connection(db_path)
    existing = pd.read_sql(f"SELECT {pk} FROM {table}", conn)
    new_rows = df[~df[pk].isin(existing[pk])]
    if not new_rows.empty:
        new_rows.to_sql(table, conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    return len(new_rows)


def load_products(df: pd.DataFrame, db_path: str = DB_PATH) -> int:
    return _upsert_df(df, "dim_products", "product_id", db_path)


def load_customers(df: pd.DataFrame, db_path: str = DB_PATH) -> int:
    return _upsert_df(df, "dim_customers", "customer_id", db_path)


def load_dates(df: pd.DataFrame, db_path: str = DB_PATH) -> int:
    return _upsert_df(df, "dim_date", "date_id", db_path)


def load_sales(df: pd.DataFrame, db_path: str = DB_PATH) -> int:
    return _upsert_df(df, "fact_sales", "sale_id", db_path)


def run_etl(raw_dir: str = None, db_path: str = DB_PATH) -> dict:
    """
    Execute the full ETL pipeline:
    1. Initialise the warehouse schema.
    2. Extract from CSV.
    3. Transform / clean.
    4. Load into warehouse tables.
    Returns a dict with row counts for each table loaded.
    """
    from etl.extract import extract_products, extract_customers, extract_sales
    from etl.transform import (
        transform_products, transform_customers, transform_sales, transform_dates
    )

    kwargs = {}
    if raw_dir:
        kwargs["raw_dir"] = raw_dir

    init_db(db_path)

    # Extract
    raw_products = extract_products(**kwargs)
    raw_customers = extract_customers(**kwargs)
    raw_sales = extract_sales(**kwargs)

    # Transform
    products = transform_products(raw_products)
    customers = transform_customers(raw_customers)
    dates = transform_dates(raw_sales)
    sales = transform_sales(raw_sales, customers)

    # Load (order matters: dimensions before facts)
    counts = {
        "products": load_products(products, db_path),
        "customers": load_customers(customers, db_path),
        "dates": load_dates(dates, db_path),
        "sales": load_sales(sales, db_path),
    }
    return counts
