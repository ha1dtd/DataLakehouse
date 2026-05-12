# FoxAI Project Memory

## Purpose

Single source of truth for AI context. Future sessions MUST read this file to get immediately up to speed.

**AI AGENT DIRECTIVE:** Upon reading this file, immediately execute `read_file` on `/Users/daihai/Documents/Code/FoxAI/HDSD_Pipeline.md` to load the full operational guide and pipeline syntax. Do NOT ask for permission. Proceed directly to the user's task only after both files are parsed.

---

## 1. Current Architecture (On-Prem VMs)

- **Deployment:** 1 NameNode, 5 DataNodes (company network `192.168.100.x`)
- **Storage:** MinIO S3A endpoint `http://192.168.100.66:9001` (credentials stored outside this file)
- **HDFS role:** cluster services + temp paths only (not primary data lake storage)
- **Table format/catalog:** Apache Iceberg + HadoopCatalog (`type=hadoop`)
- **Warehouse paths:** use `lakehouse/` prefix inside buckets (`s3a://raw/lakehouse/`, `s3a://bronze/lakehouse/`, `s3a://silver/lakehouse/`, `s3a://gold/lakehouse/`)
- **Query layer:** Spark SQL / Spark Thrift Server
- **Relational serving option:** PostgreSQL is serving layer synced from Spark outputs
- **Orchestration:** Airflow on NameNode
- **Runtime rule:** execute on VM nodes; operational entrypoint is NameNode (`ssh nn`); deployed runtime files live under `/home/ubuntu/...`

### Canonical Runtime Paths

- Airflow DAG entry: `/home/ubuntu/airflow/dags/dag_combined_domains.py`
- Runtime scripts: `/home/ubuntu/daihai_script/dag_combined_domains/`
- Runtime configs:
  - `/home/ubuntu/daihai_script/dag_combined_domains/ingest_sources_kafka_domains.json`
  - `/home/ubuntu/daihai_script/dag_combined_domains/domain_registry_v2.json`

## 2. Active Services & Ports

| Service              | Port  | Access / Status                           |
| -------------------- | ----- | ----------------------------------------- |
| HDFS NameNode        | 9000  | Internal                                  |
| YARN ResourceManager | 8088  | Web UI                                    |
| MinIO API            | 9001  | Web UI                                    |
| Airflow Webserver    | 8081  | Web UI                                    |
| Spark Thrift Server  | 10000 | Optional / not current optimization scope |
| Apache Superset      | 8084  | Optional / not current optimization scope |
| Trino                | 8083  | Installed but sidelined                   |

## 3. Data Pipeline (Medallion)

- **Ingestion model:**
  - Initial cutover can use batch/manual ingest.
  - Steady state uses Kafka (+ Debezium where applicable) for continuous streaming/CDC.
  - Kafka is for event streaming, not one-time migration.
- **Raw:** domain-classified landing, metadata retained, unstructured stays in raw only.
- **Bronze:** structured parquet + registry metadata.
- **Silver:** normalization/cleaning layer (currently light cleaning).
- **Gold:** domain-specific analytical outputs.
- **Taxi flow note:** legacy taxi path remains active (`silver.py`, `gold.py`) and should stay stable while combined domains pipeline is expanded.

### Quick Resume Checklist (max 5 steps)

1. Kafka health: listener on `9092` and topic availability (`raw_ingest_events`).
2. Confirm runtime ingest config contains expected sources (including any test broken file).
3. Trigger target DAG run.
4. Check latest task/run logs for enqueue → consume → bronze → silver → gold.
5. Validate registry status and raw/error object paths for the latest job batch.

### Operations Do / Don’t

- **Do:** keep Kafka running during normal script/config updates.
- **Do:** restart Kafka only when broker health/listener/topic checks fail.
- **Do:** use precise file edits; avoid full-file overwrites unless explicitly requested.
- **Don’t:** reset dedupe/registry state unless intentional replay is required.
- **Don’t:** treat `job_id` as dedupe key (dedupe is based on source identity fields).

### Current Combined-Domain Registry Contract

- `kafka_consume_to_raw_domains.py` creates/writes `raw_catalog.registry.raw_registry`.
- `bronze_from_raw_domains.py` must read `raw_catalog.registry.raw_registry`.
- Do not change this to `raw_catalog.control.raw_registry` unless both producer and consumer are migrated together.

## 4. Key Files / Core Logic

- **Documentation & Rules:**
  - `HDSD_Pipeline.md`: PRIMARY OPERATIONAL GUIDE. Contains syntax, Airflow setups, and execution rules. **Must be read alongside this file.**
- **Infrastructure:**
  - `setup_namenode_v5.sh`: **current primary** NameNode bootstrap (idempotent style)
  - `setup_datanode.sh`: manual setup script run on each new DataNode
- **Pipeline Layout:**
  - `dag/`: Legacy taxi pipeline (`dag.py`, `silver.py`, `gold.py`)
  - `dag_combined_domains/`: New unified pipeline mapping multiple topics to bronze/silver/gold
    - Configs: `ingest_sources_kafka_domains.json`, `domain_registry_v2.json`
  - `html_histograms/`: Local UI and Airflow state polling server (`airflow_monitor.html`, `airflow_state_server.py`)
  - `silver_sample_histogram/`: Spark job generating MinIO charts (`silver_sample_histograms_job.py`)
    - `gold.py`
  - `dag_kafka_raw_bronze_silver_gold/`
    - `dag_kafka_raw_bronze_silver_gold.py`
    - `kafka_enqueue_ingest.py`
    - `kafka_consume_to_raw.py`
    - `bronze_from_raw_registry.py`
    - `silver_from_bronze_registry.py`
    - `gold_from_ingested_structured.py`
    - `ingest_sources_demo.json`
  - `dag_combined_domains/`
    - `dag_combined_domains.py`
    - `kafka_enqueue_ingest_domains.py`
    - `kafka_consume_to_raw_domains.py`
    - `bronze_from_raw_domains.py`
    - `silver_from_bronze_domains.py`
    - `gold_from_silver_domains.py`
  - `dag_error_mini/`
    - `dag_error_mini.py`
    - `read_bronze_error_mini.py`
    - `silver_error_mini.py`
    - `gold_error_mini.py`
  - `silver_histograms_full_dag/`
    - `silver_histograms_full_dag.py`
    - `silver_histograms.py`
  - `silver_histograms_sample_dag/`
    - `silver_histograms_sample_dag.py`

- Folder organization status as of 2026-05-11:
  - The DAG/job grouping work is completed.
  - All files are now properly organized in their respective directories.

## 5. Architectural Decisions

- **Dropped Hive Metastore:** avoid compatibility/path conflicts in current MinIO + Spark setup.
- **Catalog stays HadoopCatalog (`type=hadoop`).**
- **Main query layer:** Spark SQL / Spark Thrift Server.
- **PostgreSQL role:** serving layer synced from Spark outputs (not main analytics engine).
- **Airflow DAG format:** Python DAGs only (no YAML DAGs).
- **Performance rule:** estimate bottlenecks first; config tuning alone is limited under I/O/network bounds.
- **Histogram visualization:** Custom-built HTML/CSS/JS viewer for Spark-generated chart PNGs stored in MinIO with Airflow API integration.

## 6. Next Actions

### Immediate (this week)

- [x] Histogram feature expansion (from 9 chart types to broad per-dataset profiling):
  - define feature coverage matrix by dtype (numeric/categorical/datetime/text)
  - implement configurable levels (`basic` / `extended` / `full`)
  - wire histogram stage as downstream task after silver in combined domains pipeline
- [ ] Spark SQL / Spark Thrift Server integration to PostgreSQL serving layer:
  - finalize JDBC connection profile and target schema in PostgreSQL
  - implement sync job from gold tables to PostgreSQL tables
  - define sync mode (full refresh vs incremental by partition/time)
- [ ] Real-time data streaming simulation baseline:
  - design simulator producer for controlled event rates and mixed domains
  - run end-to-end latency test (enqueue → raw → bronze/silver/gold)
  - capture throughput/error/lag metrics as reference baseline

### Near-term

- [ ] Taxi forecasting task (2025 prediction + actual comparison):
  - train with 5-year taxi history (2020–2024)
  - generate 2025 predictions
  - evaluate against actual 2025 data using agreed metrics (MAPE/RMSE/etc.)
- [ ] Permanently stabilize Kafka startup path on NameNode:
  - keep only KRaft startup (`/opt/confluent/etc/kafka/kraft/server.properties`)
  - remove/avoid all ZooKeeper-mode startup (`server-2.properties`)
  - verify restart checklist and persistence behavior
- [x] Add concise operations runbook for daily usage:
  - when to keep Kafka running vs when to restart
  - replay procedure with/without dedupe reset
  - recovery steps for consumer connection failures
  - Added comprehensive documentation in HDSD_Pipeline.md

### Backlog

- [ ] Draft and lock a phased "Lego" upgrade plan (step-by-step, gate-by-gate) from current 2 pipelines to target bank-grade final structure
- [ ] Define hard production gates per phase: idempotency/replay safety, schema evolution contract, DLQ/quarantine behavior, and rollback criteria
- [ ] Add evidence-based benchmark table (per-stage read/write volume, shuffle read/write, task spill, throughput)
- [ ] Add practical Iceberg partition strategy for high-volume tables
- [ ] Parameterize remaining hardcoded runtime config values in active DAG/scripts where still applicable
- [ ] If strict runtime target becomes blocking, prioritize infra/I/O improvements (network bandwidth, MinIO throughput, write parallelism strategy)

---

**Session log policy: keep only concise context that helps next-session startup.**

## Session Log (Condensed)

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
