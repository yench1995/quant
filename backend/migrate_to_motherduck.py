"""One-time script to migrate local DuckDB data to MotherDuck.

Usage:
    export MOTHERDUCK_TOKEN=<your_token>
    python migrate_to_motherduck.py

Run from the backend/ directory. Requires motherduck package:
    pip install duckdb  # v0.10+ includes MotherDuck support
"""

import os
import sys
import duckdb
from pathlib import Path

LOCAL_PATH = Path(__file__).parent / "../data/finance.duckdb"
CLOUD_DB = "md:finance"

TABLES = [
    "lhb_daily",
    "stock_price_daily",
    "stock_indicator_daily",
    "backtest_runs",
    "backtest_results",
    "trades",
]


def main():
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        print("ERROR: MOTHERDUCK_TOKEN env var not set.")
        sys.exit(1)

    if not LOCAL_PATH.exists():
        print(f"ERROR: Local DB not found at {LOCAL_PATH.resolve()}")
        sys.exit(1)

    print(f"Connecting to local DB: {LOCAL_PATH.resolve()}")
    local = duckdb.connect(str(LOCAL_PATH.resolve()))

    print(f"Connecting to MotherDuck: {CLOUD_DB}")
    cloud = duckdb.connect(CLOUD_DB)

    for table in TABLES:
        try:
            df = local.execute(f"SELECT * FROM {table}").df()
            if df.empty:
                print(f"  {table}: empty, skipping")
                continue
            cloud.execute(f"INSERT INTO {table} SELECT * FROM df ON CONFLICT DO NOTHING")
            print(f"  {table}: migrated {len(df):,} rows")
        except Exception as e:
            print(f"  {table}: FAILED — {e}")

    local.close()
    cloud.close()
    print("\nMigration complete.")


if __name__ == "__main__":
    main()
