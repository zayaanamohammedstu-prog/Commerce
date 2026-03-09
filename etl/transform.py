"""
etl/transform.py
Clean and enrich raw DataFrames to match the data warehouse schema.
"""
import pandas as pd


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean the products DataFrame."""
    df = df.copy()
    # Drop rows missing required fields
    df.dropna(subset=["product_id", "name", "category", "unit_price"], inplace=True)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df.dropna(subset=["unit_price"], inplace=True)
    # Normalise text
    df["name"] = df["name"].str.strip()
    df["category"] = df["category"].str.strip()
    return df[["product_id", "name", "category", "unit_price"]].drop_duplicates("product_id")


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean the customers DataFrame."""
    df = df.copy()
    df.dropna(subset=["customer_id", "name", "region", "country"], inplace=True)
    df["name"] = df["name"].str.strip()
    df["region"] = df["region"].str.strip()
    df["country"] = df["country"].str.strip()
    df["email"] = df["email"].str.strip().str.lower()
    return df[["customer_id", "name", "email", "region", "country"]].drop_duplicates("customer_id")


def _build_date_row(d: pd.Timestamp) -> dict:
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return {
        "date_id": d.strftime("%Y-%m-%d"),
        "year": d.year,
        "quarter": (d.month - 1) // 3 + 1,
        "month": d.month,
        "month_name": month_names[d.month - 1],
        "week": int(d.strftime("%W")),
        "day": d.day,
        "day_name": day_names[d.weekday()],
        "is_weekend": 1 if d.weekday() >= 5 else 0,
    }


def transform_dates(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Generate the date dimension from all unique dates in the sales data."""
    dates = pd.to_datetime(sales_df["date"].dropna().unique())
    rows = [_build_date_row(d) for d in sorted(dates)]
    return pd.DataFrame(rows)


def transform_sales(sales_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Clean sales data and enrich with region from customers."""
    df = sales_df.copy()
    df.dropna(subset=["sale_id", "date", "product_id", "customer_id"], inplace=True)

    # Parse numeric fields
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    df.dropna(subset=["quantity", "unit_price", "total_amount"], inplace=True)

    # Filter out non-positive values
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] > 0]
    df = df[df["total_amount"] > 0]

    # Enrich with region from customers
    region_map = customers_df.set_index("customer_id")["region"].to_dict()
    df["region"] = df["customer_id"].map(region_map).fillna("Unknown")

    # Normalise date column
    df["date_id"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    return df[["sale_id", "date_id", "product_id", "customer_id",
               "quantity", "unit_price", "total_amount", "region"]].drop_duplicates("sale_id")
