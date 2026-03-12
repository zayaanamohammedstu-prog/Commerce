"""
etl/client_pipeline.py
ETL pipeline for client-uploaded CSV/Excel sales files.
Accepts a file path, cleans/transforms the data, and loads into the client's warehouse DB.
"""
import os
import uuid
import pandas as pd
from sqlalchemy import create_engine, text
from warehouse.database import init_db


ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MB


def allowed_file(filename: str) -> bool:
    ext = os.path.splitext(filename.lower())[1]
    return ext in ALLOWED_EXTENSIONS


def read_uploaded_file(file_path: str) -> pd.DataFrame:
    """Read CSV or Excel file into a DataFrame."""
    ext = os.path.splitext(file_path.lower())[1]
    if ext == ".csv":
        df = pd.read_csv(file_path)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip column names."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    return df


def _resolve_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """Find and rename the date column to 'date'."""
    date_candidates = ["date", "order_date", "sale_date", "transaction_date", "purchase_date"]
    for col in date_candidates:
        if col in df.columns:
            if col != "date":
                df = df.rename(columns={col: "date"})
            break
    else:
        raise ValueError("Could not find a date column. Expected: date, order_date, sale_date, transaction_date, or purchase_date.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    return df


def _resolve_revenue_column(df: pd.DataFrame) -> pd.DataFrame:
    """Resolve revenue from possible column variants."""
    if "revenue" not in df.columns:
        if "total_amount" in df.columns:
            df["revenue"] = pd.to_numeric(df["total_amount"], errors="coerce")
        elif "quantity" in df.columns and "unit_price" in df.columns:
            df["revenue"] = (
                pd.to_numeric(df["quantity"], errors="coerce")
                * pd.to_numeric(df["unit_price"], errors="coerce")
            )
        elif "amount" in df.columns:
            df["revenue"] = pd.to_numeric(df["amount"], errors="coerce")
        elif "sales" in df.columns:
            df["revenue"] = pd.to_numeric(df["sales"], errors="coerce")
        else:
            raise ValueError(
                "Could not find a revenue column. Expected one of: revenue, total_amount, amount, sales, or quantity+unit_price."
            )
    else:
        df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df["revenue"] = df["revenue"].fillna(0.0)
    return df


def _build_dimensions(df: pd.DataFrame):
    """Build dim_products, dim_customers, dim_date from the raw data."""
    # Products
    if "product_id" in df.columns and "product_name" in df.columns:
        products = df[["product_id", "product_name"]].drop_duplicates("product_id").copy()
        products = products.rename(columns={"product_name": "name"})
    elif "product_id" in df.columns:
        products = df[["product_id"]].drop_duplicates("product_id").copy()
        products["name"] = products["product_id"]
    else:
        # Generate product IDs from category or set default
        cat_col = "category" if "category" in df.columns else None
        df["product_id"] = df.apply(
            lambda r: f"PROD_{r[cat_col][:8].upper().replace(' ', '_')}" if cat_col else "PROD_DEFAULT",
            axis=1
        )
        products = df[["product_id"]].drop_duplicates().copy()
        products["name"] = products["product_id"]

    if "category" not in products.columns:
        if "category" in df.columns:
            cat_map = df[["product_id", "category"]].drop_duplicates("product_id").set_index("product_id")["category"].to_dict()
            products["category"] = products["product_id"].map(cat_map).fillna("General")
        else:
            products["category"] = "General"

    if "unit_price" not in products.columns:
        if "unit_price" in df.columns:
            price_map = df[["product_id", "unit_price"]].drop_duplicates("product_id").set_index("product_id")["unit_price"].to_dict()
            products["unit_price"] = products["product_id"].map(price_map).fillna(0.0)
        else:
            products["unit_price"] = 0.0

    # Customers
    if "customer_id" in df.columns:
        cust_cols = ["customer_id"]
        if "customer_name" in df.columns:
            cust_cols.append("customer_name")
        if "region" in df.columns:
            cust_cols.append("region")
        if "country" in df.columns:
            cust_cols.append("country")
        customers = df[cust_cols].drop_duplicates("customer_id").copy()
        if "customer_name" in customers.columns:
            customers = customers.rename(columns={"customer_name": "name"})
        else:
            customers["name"] = customers["customer_id"]
    else:
        df["customer_id"] = "CUST_DEFAULT"
        customers = pd.DataFrame([{"customer_id": "CUST_DEFAULT", "name": "Default Customer"}])

    if "region" not in customers.columns:
        if "region" in df.columns:
            region_map = df[["customer_id", "region"]].drop_duplicates("customer_id").set_index("customer_id")["region"].to_dict()
            customers["region"] = customers["customer_id"].map(region_map).fillna("Unknown")
        else:
            customers["region"] = "Unknown"
    if "country" not in customers.columns:
        customers["country"] = "Unknown"
    if "email" not in customers.columns:
        customers["email"] = ""

    # Dates
    dates_list = []
    month_names = ["January","February","March","April","May","June","July","August","September","October","November","December"]
    day_names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    for d in df["date"].dt.normalize().unique():
        ts = pd.Timestamp(d)
        dates_list.append({
            "date_id": ts.strftime("%Y-%m-%d"),
            "year": ts.year,
            "quarter": (ts.month - 1) // 3 + 1,
            "month": ts.month,
            "month_name": month_names[ts.month - 1],
            "week": int(ts.strftime("%W")),
            "day": ts.day,
            "day_name": day_names[ts.weekday()],
            "is_weekend": 1 if ts.weekday() >= 5 else 0,
        })
    dates = pd.DataFrame(dates_list)

    return products, customers, dates


def _build_facts(df: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """Build fact_sales from cleaned data."""
    region_map = customers.set_index("customer_id")["region"].to_dict() if "customer_id" in customers.columns else {}

    facts = df.copy()
    facts["date_id"] = facts["date"].dt.strftime("%Y-%m-%d")
    if "customer_id" not in facts.columns:
        facts["customer_id"] = "CUST_DEFAULT"
    if "product_id" not in facts.columns:
        facts["product_id"] = "PROD_DEFAULT"
    if "quantity" not in facts.columns:
        facts["quantity"] = 1
    facts["quantity"] = pd.to_numeric(facts["quantity"], errors="coerce").fillna(1).astype(int)
    if "unit_price" not in facts.columns:
        facts["unit_price"] = facts["revenue"]
    facts["unit_price"] = pd.to_numeric(facts["unit_price"], errors="coerce").fillna(0.0)
    facts["total_amount"] = facts["revenue"]
    facts["region"] = facts["customer_id"].map(region_map).fillna("Unknown")
    if "region" in df.columns:
        facts["region"] = df["region"].fillna(facts["region"])

    # Generate unique sale_ids
    facts["sale_id"] = [f"SALE_{uuid.uuid4().hex[:12]}" for _ in range(len(facts))]

    return facts[["sale_id", "date_id", "product_id", "customer_id",
                  "quantity", "unit_price", "total_amount", "region"]].copy()


def run_client_etl(file_path: str, client_db_path: str) -> dict:
    """
    Full ETL for a client-uploaded file.
    Returns dict with counts of rows loaded.
    """
    init_db(client_db_path)

    df = read_uploaded_file(file_path)
    df = _normalize_columns(df)
    df = _resolve_date_column(df)
    df = _resolve_revenue_column(df)

    if df.empty:
        raise ValueError("No valid rows found after cleaning.")

    products, customers, dates = _build_dimensions(df)
    facts = _build_facts(df, customers)

    engine = create_engine(f"sqlite:///{client_db_path}")

    def _upsert(frame: pd.DataFrame, table: str, pk: str):
        import sqlite3 as _sq3
        conn = _sq3.connect(client_db_path)
        existing_ids = pd.read_sql(f"SELECT {pk} FROM {table}", conn)[pk].tolist()
        conn.close()
        new_rows = frame[~frame[pk].isin(existing_ids)]
        if not new_rows.empty:
            with engine.begin() as con:
                new_rows.to_sql(table, con, if_exists="append", index=False)
        return len(new_rows)

    counts = {
        "products": _upsert(products[["product_id","name","category","unit_price"]].drop_duplicates("product_id"), "dim_products", "product_id"),
        "customers": _upsert(customers[["customer_id","name","email","region","country"]].drop_duplicates("customer_id"), "dim_customers", "customer_id"),
        "dates": _upsert(dates, "dim_date", "date_id"),
        "sales": _upsert(facts, "fact_sales", "sale_id"),
    }
    return counts
