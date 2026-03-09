"""
run.py
Entry point: run the ETL pipeline then start the Flask web application.
"""
import os
import sys

# Ensure imports work from repo root
sys.path.insert(0, os.path.dirname(__file__))

from etl.load import run_etl
from api.app import app

if __name__ == "__main__":
    print("=== Commerce Analytics ===")
    print("Running ETL pipeline…")
    counts = run_etl()
    print(f"  Loaded: {counts}")
    print("Starting web server on http://localhost:5000")
    app.run(debug=False, host="0.0.0.0", port=5000)
