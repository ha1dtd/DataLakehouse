# FoxAI Project Memory

## Purpose

Long-form project context for future sessions — architecture, active systems, decisions, progress.
Not a rule file. Rules live in the separate rules.md.

---

## 1. Current Active Work

### Primary focus

- Realtime histogram demo for `fare_amount`
- Flow: inbox files → Kafka → Airflow DAG → MinIO state → Spark histogram snapshot → HTML viewer
- Goal: stable end-to-end demo with correct UI behavior and exact Kafka-first semantics

### Open items

- Verify `airflow_monitor.html` refresh behavior after latest refactor:
  - recent runs refresh during active run
  - task/state/log refresh through one coherent live path only
  - refresh stops when no task is active
- PostgreSQL serving-layer integration via Spark / Thrift / JDBC
- Reproducible realtime streaming baseline metrics

---

## 2. Infrastructure

| Component     | Detail                                          |
| ------------- | ----------------------------------------------- |
| Cluster       | 1 NameNode + 5 DataNodes on `192.168.100.x`     |
| Storage       | MinIO `http://192.168.100.66:9001`              |
| Orchestration | Airflow `http://192.168.100.66:8081`            |
| Processing    | Spark on YARN                                   |
| Table format  | Iceberg + HadoopCatalog (`type=hadoop`)         |
| HDFS role     | Cluster/runtime support only — not lake storage |

### Active ports

| Service              | Port  | Status               |
| -------------------- | ----- | -------------------- |
| HDFS NameNode        | 9000  | Internal             |
| YARN ResourceManager | 8088  | Web UI               |
| MinIO API            | 9001  | Web UI               |
| Airflow              | 8081  | Web UI               |
| Spark Thrift Server  | 10000 | Optional             |
| Apache Superset      | 8084  | Optional             |
| Trino                | 8083  | Installed, sidelined |

---

## 3. Active Systems

### A. Combined-domain medallion pipeline

- Flow: Kafka → raw registry → bronze → silver → gold
- DAG: `/home/ubuntu/airflow/dags/dag_combined_domains.py`
- Scripts: `/home/ubuntu/daihai_script/dag_combined_domains/`
- Configs:
  - `ingest_sources_kafka_domains.json`
  - `domain_registry_v2.json`
- Registry contract: `kafka_consume_to_raw_domains.py` writes `raw_catalog.registry.raw_registry` — `bronze_from_raw_domains.py` must read the same table

### B. Realtime histogram demo

- DAG id: `realtime_fare_amount_pipeline`
- Kafka broker: `192.168.100.66:9092`
- Topic: `realtime_fare_amount_demo`
- Consumer group: `realtime-fare-amount-demo-airflow`
- DAG file: `/home/ubuntu/airflow/dags/realtime_fare_amount_single_dag.py`
- Scripts: `/home/ubuntu/daihai_script/realtime_histogram_demo/`
- Viewer HTML: `/home/ubuntu/daihai_script/html_histograms/histogram_chat_viewer.html`
- Inbox: `inbox/rows` (single rows), `inbox/batch` (batch files)
- Airflow tasks: `consume_kafka_and_update_minio_state` → `gate_histogram_if_new_data` → `generate_histogram_snapshot`
- Snapshot output: `demo/<snapshot>/fare_amount/`
- Persistent state: `demo/realtime_fare_amount/state/`

---

## 4. Key Local Files

### Frontend

- `html/histogram_chat_viewer.html`
- `html/airflow_monitor.html` + `airflow_monitor.css`
- `html/_dark_mode.css`

### Realtime demo scripts

- `realtime_histogram_demo/realtime_fare_amount_single_dag.py`
- `realtime_histogram_demo/realtime_fare_amount_kafka_consume_and_update_v2.py`
- `realtime_histogram_demo/realtime_fare_amount_histogram_job.py`
- `realtime_histogram_demo/realtime_fare_amount_inbox_poller.py`
- `realtime_histogram_demo/reset_realtime_fare_amount_demo.sh`

### Platform / legacy

- `dag/` — legacy taxi pipeline
- `dag_combined_domains/` — combined-domain pipeline
- `silver_histograms_dag/` / `silver_sample_histogram/` — histogram jobs

---

## 5. Recently Completed

- Kafka-first realtime histogram demo — full flow built and verified
- Poller support for `.json`, `.csv`, `.parquet`, `.xml`
- Fixed bootstrap vs consumer-group offset behavior
- Retry-safe histogram gate logic
- Flattened demo output path to `demo/<snapshot>/fare_amount/...`
- Viewer path logic synced with flattened structure
- Reset helper for MinIO state + Kafka replay
- Fixed registry mismatch in combined-domain bronze read path
- Airflow monitor: improved log copy, partial refresh refactor (verification pending)

---

## 6. Near-term / Backlog

### Near-term

- Stabilize Kafka KRaft-only startup runbook on namenode
- Taxi 2025 forecasting (train on 2020–2024, evaluate vs actuals)
- Parameterize remaining hardcoded values in active scripts

### Longer-term

- Licensing system (integrate with other team's API)
- Binary packaging for Linux distribution
- Plugin SDK (CustomTransformer v1)
- Low-latency realtime processing track

---

## 7. Key Decisions & Constraints

- Hive Metastore dropped — HadoopCatalog is the current path
- Kafka is streaming infrastructure, not one-time migration tooling
- Performance is I/O/network bound — config tuning alone has limited impact
- Legacy taxi pipeline stays stable while newer pipelines expand alongside it
- Airflow: Python DAGs only
- Edits stay targeted — no broad overwrites, no parallel copies of active scripts

---

## 8. Resume Checklist

1. Which system is active? Combined-domain pipeline or realtime histogram demo?
2. For realtime demo: confirm Kafka topic, consumer group, DAG id, and target runtime files
3. Read relevant active code before editing
4. After namenode push, verify remote file content
5. Update this file only when architecture or progress meaningfully changes
