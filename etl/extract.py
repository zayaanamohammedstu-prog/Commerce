"""
etl/extract.py
Read raw CSV files from data/raw/ and return DataFrames.
"""
import os
import pandas as pd

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


def extract_products(raw_dir: str = RAW_DIR) -> pd.DataFrame:
    path = os.path.join(raw_dir, "products.csv")
    df = pd.read_csv(path, dtype=str)
    return df


def extract_customers(raw_dir: str = RAW_DIR) -> pd.DataFrame:
    path = os.path.join(raw_dir, "customers.csv")
    df = pd.read_csv(path, dtype=str)
    return df


def extract_sales(raw_dir: str = RAW_DIR) -> pd.DataFrame:
    path = os.path.join(raw_dir, "sales.csv")
    df = pd.read_csv(path, dtype=str)
    return df
