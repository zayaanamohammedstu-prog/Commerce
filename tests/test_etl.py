"""
tests/test_etl.py
Tests for the ETL pipeline (extract, transform, load).
"""
import os
import sys
import tempfile
import pytest
import pandas as pd

# Ensure repo root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.transform import (
    transform_products,
    transform_customers,
    transform_sales,
    transform_dates,
)
from etl.load import run_etl
from warehouse.database import init_db, drop_all


# ── Fixtures ─────────────────────────────────────────────────────────────────

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


@pytest.fixture
def sample_products():
    return pd.DataFrame([
        {"product_id": "P001", "name": "  Laptop ", "category": "Electronics", "unit_price": "1299.99"},
        {"product_id": "P002", "name": "Mouse",     "category": "Electronics", "unit_price": "bad"},
        {"product_id": "P003", "name": "Desk",      "category": "Furniture",   "unit_price": "499.99"},
    ])


@pytest.fixture
def sample_customers():
    return pd.DataFrame([
        {"customer_id": "C001", "name": "Alice", "email": "ALICE@EXAMPLE.COM", "region": "North America", "country": "USA"},
        {"customer_id": "C002", "name": "Bob",   "email": "bob@example.com",   "region": "Europe",        "country": "UK"},
    ])


@pytest.fixture
def sample_sales():
    return pd.DataFrame([
        {"sale_id": "S001", "date": "2024-01-15", "product_id": "P001", "customer_id": "C001",
         "quantity": "2", "unit_price": "1200.00", "total_amount": "2400.00"},
        {"sale_id": "S002", "date": "2024-01-16", "product_id": "P003", "customer_id": "C002",
         "quantity": "1", "unit_price": "499.99",  "total_amount": "499.99"},
        # Bad row: negative quantity
        {"sale_id": "S003", "date": "2024-02-01", "product_id": "P001", "customer_id": "C001",
         "quantity": "-1", "unit_price": "1200.00", "total_amount": "-1200.00"},
    ])


# ── Transform: Products ───────────────────────────────────────────────────────

def test_transform_products_cleans_whitespace(sample_products):
    result = transform_products(sample_products)
    assert result.loc[result.product_id == "P001", "name"].iloc[0] == "Laptop"


def test_transform_products_drops_bad_price(sample_products):
    result = transform_products(sample_products)
    assert "P002" not in result["product_id"].values


def test_transform_products_valid_rows(sample_products):
    result = transform_products(sample_products)
    assert set(result["product_id"].tolist()) == {"P001", "P003"}


# ── Transform: Customers ─────────────────────────────────────────────────────

def test_transform_customers_lowercase_email(sample_customers):
    result = transform_customers(sample_customers)
    emails = result["email"].tolist()
    assert all(e == e.lower() for e in emails)


def test_transform_customers_row_count(sample_customers):
    result = transform_customers(sample_customers)
    assert len(result) == 2


# ── Transform: Dates ─────────────────────────────────────────────────────────

def test_transform_dates_columns(sample_sales):
    result = transform_dates(sample_sales)
    expected_cols = {"date_id", "year", "quarter", "month", "month_name", "week", "day", "day_name", "is_weekend"}
    assert expected_cols.issubset(set(result.columns))


def test_transform_dates_unique(sample_sales):
    result = transform_dates(sample_sales)
    assert result["date_id"].is_unique


def test_transform_dates_correct_quarter(sample_sales):
    result = transform_dates(sample_sales)
    jan_row = result[result["date_id"] == "2024-01-15"].iloc[0]
    assert jan_row["quarter"] == 1


# ── Transform: Sales ─────────────────────────────────────────────────────────

def test_transform_sales_drops_negative_quantity(sample_sales, sample_customers):
    customers = transform_customers(sample_customers)
    result = transform_sales(sample_sales, customers)
    assert "S003" not in result["sale_id"].values


def test_transform_sales_enriches_region(sample_sales, sample_customers):
    customers = transform_customers(sample_customers)
    result = transform_sales(sample_sales, customers)
    row = result[result["sale_id"] == "S001"].iloc[0]
    assert row["region"] == "North America"


def test_transform_sales_has_date_id(sample_sales, sample_customers):
    customers = transform_customers(sample_customers)
    result = transform_sales(sample_sales, customers)
    assert "date_id" in result.columns
    assert result["date_id"].iloc[0] == "2024-01-15"


# ── End-to-end ETL with real CSV files ────────────────────────────────────────

def test_run_etl_populates_warehouse():
    """Full ETL run against the real CSV files into a temp DB."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    try:
        counts = run_etl(raw_dir=RAW_DIR, db_path=db_path)
        assert counts["products"] > 0
        assert counts["customers"] > 0
        assert counts["sales"] > 0
        assert counts["dates"] > 0
    finally:
        os.unlink(db_path)


def test_run_etl_idempotent():
    """Running ETL twice should not duplicate rows."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name
    try:
        counts1 = run_etl(raw_dir=RAW_DIR, db_path=db_path)
        counts2 = run_etl(raw_dir=RAW_DIR, db_path=db_path)
        # Second run should load zero new rows
        assert counts2["sales"] == 0
        assert counts2["products"] == 0
    finally:
        os.unlink(db_path)
