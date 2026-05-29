# Starter Add-on

This is a generic starter package for building a new customer add-on from scratch.

It is intentionally generic, but its structure mirrors the real Lakehouse setup pattern used by our current medallion DAG packages:

- one Airflow DAG entrypoint
- staged jobs:
  - `postgres_to_raw.py`
  - `raw_to_bronze.py`
  - `bronze_to_silver.py`
  - `silver_to_gold.py`
- structured config under `config/`
- optional SQL and future service folders
