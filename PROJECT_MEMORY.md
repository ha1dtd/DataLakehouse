# FoxAI Project Memory

## Purpose

Single source of truth for AI context. Future sessions read this file to get immediately up to speed.

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
- Runtime scripts: `/home/ubuntu/daihai_script/
- Runtime configs:
  - `/home/ubuntu/daihai_script/dag_combined_domains/ingest_sources_kafka_domains.json`
  - `/home/ubuntu/daihai_script/dag_combined_domains/domain_registry_v2.json`

## 2. Active Services & Ports

| Service              | Port  | Access / Status                           |
| -------------------- | ----- | ----------------------------------------- |
| HDFS NameNode        | 9000  | Internal                                  |
| YARN ResourceManager | 8088  | Web UI                                    |
| MinIO API            | 9001  | Web UI                                    |
| Airflow Webserver    | 8080  | Web UI                                    |
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
- **Don’t:** reset dedupe/registry state unless intentional replay is required.
- **Don’t:** treat `job_id` as dedupe key (dedupe is based on source identity fields).

## 4. Key Files / Current Folder Organization

- `setup_namenode_v5.sh`: **current primary** NameNode bootstrap for 2-existing + 3-new DataNode expansion (idempotent style)
- `setup_datanode.sh`: manual setup script run on each new DataNode after NameNode setup
- `setup_namenode_v4.sh`: previous baseline script (kept for reference)
- `HDSD_Pipeline.md`: operational guide
- Pipeline folders currently organized as:
  - `dag/`
    - `dag.py`
    - `read_bronze.py`
    - `silver.py`
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
    - `ingest_sources_kafka_domains.json` (current: 23 sources = 20 taxi parquets for 2020–2024 month-01 across 4 taxi topics + 3 added one-topic files: hr/finance/marketing)
    - `domain_registry_v2.json`
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
- Folder organization status as of 2026-04-23:
  - The DAG/job grouping work is mostly done.
  - One known unfinished detail remains: `silver_histograms_sample.py` is still at project root instead of inside `silver_histograms_sample_dag/`.

## 5. Architectural Decisions

- **Dropped Hive Metastore:** avoid compatibility/path conflicts in current MinIO + Spark setup.
- **Catalog stays HadoopCatalog (`type=hadoop`).**
- **Main query layer:** Spark SQL / Spark Thrift Server.
- **PostgreSQL role:** serving layer synced from Spark outputs (not main analytics engine).
- **Airflow DAG format:** Python DAGs only (no YAML DAGs).
- **Performance rule:** estimate bottlenecks first; config tuning alone is limited under I/O/network bounds.

## 6. Next Actions

### Immediate (this week)

- [ ] Histogram feature expansion (from 9 chart types to broad per-dataset profiling):
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
- [ ] Add concise operations runbook for daily usage:
  - when to keep Kafka running vs when to restart
  - replay procedure with/without dedupe reset
  - recovery steps for consumer connection failures

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

- **2026-04-01 → 2026-04-15:** Infrastructure and stack direction stabilized on on-prem Spark + YARN + MinIO + Airflow + Iceberg (HadoopCatalog). Hive Metastore approach was dropped.
- **2026-04-17 → 2026-04-21:** Core taxi pipeline fixes and tuning were applied (schema reconciliation, DAG/Spark settings, script rename to `silver.py`/`gold.py`). Main performance finding: workload is largely I/O/network bound, so config-only tuning gives limited wall-clock improvement.
- **2026-04-22:** Target architecture aligned to `dtlver3` direction: keep old taxi path stable, build/validate hardening through new combined pipeline (Kafka + registry + error handling).
- **2026-04-23:** DAG folder organization mostly completed; one known cleanup remained (`silver_histograms_sample.py` still at root).
- **2026-04-24:** Ingest source intent clarified and applied: keep 20 taxi sources and append 3 domain sources (`hr`, `finance`, `marketing`) in `dag_combined_domains/ingest_sources_kafka_domains.json` (23 entries total).
- **2026-04-28:** Combined domain pipeline rerun hardening completed. Key outcomes: Kafka topic `raw_ingest_events` operational in KRaft mode on `9092`; dedupe/replay behavior validated (dedupe key is `domain+topic+source_name+source_uri+file_name`, not `job_id`); broken XML source (`finance_broken_xml` / `sample_orders_broken.xml`) restored into runtime ingest config; full pipeline run succeeded with implemented features; operational rule confirmed to keep Kafka running during normal data/script updates and restart only when broker health fails.
