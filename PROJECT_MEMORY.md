# FoxAI Project Memory

## Purpose

Persistent working memory for the FoxAI project so future sessions can get up to speed quickly.

## How to use

- Capture project context, architecture, key files, decisions, blockers, and next steps.
- Keep entries concise.
- Prefer dated updates.
- Treat this file as the single source of truth for AI context.

---

## Project Snapshot

- Project folder: /Users/daihai/Documents/Code/FoxAI
- Status: End-to-end pipeline working (Bronze → Silver → Gold)
- Architecture: Lakehouse (Spark + YARN + MinIO + Airflow + Iceberg)
- **Update:** Preparing for migration to on-premise servers of company

---

## Architecture / Files

- Medallion architecture:
  - Bronze → raw data (MinIO)
  - Silver → cleaned data (Iceberg)
  - Gold → enriched dataset (Iceberg)

- ingest.sh
  - Uploads local file or URL → MinIO
  - Target: bronze/raw/

- silver.py
  - Reads: s3a://bronze/
  - Cleaning:
    - dropna(all)
    - dropDuplicates()
    - filter invalid values (no negative fares, distance, etc.)
  - Feature:
    - trip_duration_minutes
  - Writes:
    - Iceberg → silver_catalog.default.yellow_taxi

- gold.py
  - Reads:
    - silver_catalog.default.yellow_taxi
  - Feature engineering:
    - time (pickup_hour, pickup_weekday)
    - flags (is_peak_hour, is_weekend, is_suspicious)
    - financial (total_fees, tip_ratio, revenue_per_minute)
    - efficiency (cost_per_mile, avg_speed_mph)
    - classification (trip_type, is_card_payment, tipped)
  - Writes:
    - Iceberg → gold_catalog.default.yellow_taxi

- airflow.py
  - DAG: spark_hdfs_medallion_pipeline
  - Flow:
    - silver_layer >> gold_layer
  - Runs Spark jobs via YARN
  - Uses Iceberg + S3A configs

- **New scripts / planned modularization** (from metadata & domain work):
  - metadata_ingest.py → create/manage bronze_catalog.metadata
  - categorize.py → auto-classify datasets by domain
  - move_to_domain.py → move datasets to domain folders (L1 → L2)
  - bronze_to_silver.py → transform domain-specific bronze → silver
  - silver_to_gold.py → transform silver → gold per domain

---

## Infrastructure

- Google Cloud VM cluster:
  - 1 NameNode
  - 2 DataNodes
- Internal network:
  - 10.148.0.x
- MinIO:
  - API: port 9001
  - Console/UI: port 9002
- Access:
  - Local via SSH tunnel (localhost:9001)
- **Update:** Moving on to on-premise servers of company (details TBD)

---

## Decisions

- Use MinIO instead of HDFS for storage
- Use Iceberg for Silver and Gold layers
- Keep ingestion external via ingest.sh
- Avoid YAML-based DAG for now
- Future ingestion direction: Kafka (per supervisor)
- Metadata-driven domain management planned (metadata table + domain categorization + domain-specific folders)

---

## Current State

- Pipeline runs successfully end-to-end
- Airflow DAG operational
- Iceberg tables created and written
- Data flow confirmed:
  Bronze → Silver → Gold
- **Update:** Working on formalizing metadata table, domain categorization, and modular scripts for domain-aware pipeline (L0-L3)

---

## Known Issues / Limitations

- Silver reads entire s3a://bronze/ (no partitioning)
- No partitioning in Iceberg tables
- Gold is not a true aggregation layer yet (only feature engineering)
- trip_duration is recomputed in Gold (duplicate logic)
- Using createOrReplace() (not ideal for production)
- Domain-aware ingestion not yet fully implemented

---

## Next Steps

- Organize Bronze data:
  - bronze/yellow_taxi/year=YYYY/month=MM/
- Add partitioning to Iceberg tables
- Convert Gold into aggregation layer (business metrics)
- Implement Iceberg features:
  - Time Travel
  - Schema Evolution
- Integrate Kafka for ingestion layer (next architecture step)
- Finalize:
  - metadata_ingest.py
  - categorize.py
  - move_to_domain.py
  - bronze_to_silver.py
  - silver_to_gold.py
- Ensure domain-aware modular pipeline works end-to-end on on-prem servers

---

## Session Log

### 2026-04-01

- Consolidated full project context into PROJECT_MEMORY.md
- Confirmed pipeline architecture:
  Spark + YARN + MinIO + Airflow + Iceberg
- Verified:
  - ingestion via ingest.sh
  - Silver and Gold jobs working via Airflow DAG
- Identified key improvements:
  - partitioning
  - aggregation layer
  - Iceberg advanced features

### 2026-04-06

- Discussed and drafted metadata & domain-driven requirements for pipeline:
  - L0-L1: Metadata table creation & management
  - L0: Domain/term auto-classification
  - L1-L2: Folder structure abstraction
  - L2-L3: Domain-aware data landing
  - Bronze → Silver → Gold per domain
- Agreed scripts to be modular:
  - metadata_ingest.py, categorize.py, move_to_domain.py, bronze_to_silver.py, silver_to_gold.py
- Noted idempotency and Airflow parameterization requirements
- Added context of moving pipeline to on-premise servers
- Next steps:
  - Implement and test modular scripts for domain-aware ingestion
  - Ensure compatibility with current pipeline
