# Archived Project Logs

These are older project/session log entries kept for reference.

- **2026-05-13:** Realtime histogram demo context stabilized:
  - Confirmed active Kafka-first demo path: inbox files → Kafka topic `realtime_fare_amount_demo` → DAG `realtime_fare_amount_pipeline` → MinIO state → Spark histogram snapshot → HTML viewer.
  - Prepared/verified May 13 demo inputs for replay and UI validation:
    - seed batch `realtime_histogram_demo/inbox/batch/fare_amount_seed_batch.json` with 5 rows at `2026-05-13T10:00:00Z` → `2026-05-13T10:00:04Z`
    - single-row event `realtime_histogram_demo/inbox/rows/fare_amount_row_20260513T120500Z.json` at `2026-05-13T12:05:00Z`
  - Runtime identifiers locked for continuity: broker `192.168.100.66:9092`, consumer group `realtime-fare-amount-demo-airflow`, persistent state prefix `demo/realtime_fare_amount/state/`.
- **2026-05-12:** Added AI working rules to avoid broad file overwrites and keep edits targeted. Confirmed active Airflow webserver port is `8081`.
- **2026-05-11:** Debugged `combined_domain_medallion_pipeline` failure:
  - Root cause: `bronze_from_raw_domains.py` read `raw_catalog.control.raw_registry`, but `kafka_consume_to_raw_domains.py` writes `raw_catalog.registry.raw_registry`.
  - Fixed local script and rsynced to `/home/ubuntu/daihai_script/dag_combined_domains/bronze_from_raw_domains.py`.
  - Triggered run `manual_fix_20260511_152251`; `enqueue_ingest_requests` and `kafka_consume_to_raw` succeeded, `bronze_from_raw` was still running when monitoring stopped.
- **2026-05-11:** Updated project documentation to reflect current state:
  - Resolved file organization issues (`silver_histograms_sample.py`)
  - Completed histogram feature expansion with web viewer
  - Updated task statuses in Next Actions
  - Added architectural decision for histogram visualization
- **2026-05-04 → 2026-05-09:** Built histogram web viewer from scratch:
  - Created `html_histograms/` with HTML/CSS/JS viewer for Spark-generated chart PNGs stored in MinIO
  - Airflow API integration for monitoring DAG runs
  - Auto-refresh histogram viewer (polls MinIO for new charts, renders in browser)
  - Dark mode UI with bright-mode color preservation for progress/status elements
  - Integer bin & tick fixes applied to `silver_sample_histograms_job.py` and deployed to namenode
- **2026-04-01 → 2026-04-15:** Infrastructure and stack direction stabilized on on-prem Spark + YARN + MinIO + Airflow + Iceberg (HadoopCatalog). Hive Metastore approach was dropped.
- **2026-04-17 → 2026-04-21:** Core taxi pipeline fixes and tuning were applied (schema reconciliation, DAG/Spark settings, script rename to `silver.py`/`gold.py`). Main performance finding: workload is largely I/O/network bound, so config-only tuning gives limited wall-clock improvement.
- **2026-04-22:** Target architecture aligned to `dtlver3` direction: keep old taxi path stable, build/validate hardening through new combined pipeline (Kafka + registry + error handling).
- **2026-04-23:** DAG folder organization mostly completed; one known cleanup remained (`silver_histograms_sample.py` still at root).
- **2026-04-28:** Combined domain pipeline rerun hardening completed.
- **2026-04-24:** Ingest source intent clarified and applied: keep 20 taxi sources and append 3 domain sources (`hr`, `finance`, `marketing`) in `dag_combined_domains/ingest_sources_kafka_domains.json` (23 entries total).
- **2026-04-01 → 2026-04-15:** Infrastructure and stack direction stabilized on on-prem Spark + YARN + MinIO + Airflow + Iceberg (HadoopCatalog). Hive Metastore approach was dropped.
- **2026-04-17 → 2026-04-21:** Core taxi pipeline fixes and tuning were applied (schema reconciliation, DAG/Spark settings, script rename to `silver.py`/`gold.py`). Main performance finding: workload is largely I/O/network bound, so config-only tuning gives limited wall-clock improvement.
- **2026-04-22:** Target architecture aligned to `dtlver3` direction: keep old taxi path stable, build/validate hardening through new combined pipeline (Kafka + registry + error handling).
- **2026-04-23:** DAG folder organization mostly completed; one known cleanup remained (`silver_histograms_sample.py` still at root).
- **2026-04-28:** Combined domain pipeline rerun hardening completed.
- **2026-04-24:** Ingest source intent clarified and applied: keep 20 taxi sources and append 3 domain sources (`hr`, `finance`, `marketing`) in `dag_combined_domains/ingest_sources_kafka_domains.json` (23 entries total).
