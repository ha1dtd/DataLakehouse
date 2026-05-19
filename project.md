# FoxAI Project Memory

## Purpose

Long-form project context for future sessions — architecture, active systems, decisions, progress.
Not a rule file. Rules live in the separate rules.md.

---

## 1. Current Active Work

### Primary focus

- Realtime histogram demo for `fare_amount`
- Current active validation subtrack: `realtime_validate` for batch-vs-row replay correctness on the same silver parquet slice
- `realtime_validate` file mode checkpoint now complete and deployed:
  - file ingest is pointer-only
  - calculation runs in Spark via `spark-submit`
  - comparison JSON is written alongside chart output
  - duplicate rerun continues if downstream artifacts are missing
  - histogram bounds now use a two-stage IQR style upper bound for `fare_amount` instead of raw-max scaling
- Current known weak point is row-file mode performance, not file mode correctness:
  - row-file still explodes parquet into per-row messages in Python/pandas
  - row ingest still reloads and rewrites large JSON state
  - row replay remains the next fix target before full batch-vs-row validation
- Active parallel paths now:
  - Kafka demo flow: inbox files → Kafka → Airflow DAG → MinIO state → Spark histogram snapshot → HTML viewer
  - RabbitMQ demo flow: transmitter → RabbitMQ → long-lived receiver on namenode → MinIO raw event → Airflow DAG trigger → MinIO state → calculation artifact → chart snapshot
- Goal: stable end-to-end demo with correct UI behavior, exact replay semantics, and a batch-vs-realtime comparison path for drift/duplication checking

### Secondary active track

- Combined-domain medallion pipeline safe-hardening pass
- Safe fixes 1-3 are complete and deployed on namenode
- Remaining agreed safe fixes:
  - Fix 4: explicit failure-context logging before re-raise
  - Fix 5: small observability logs for phase start/end, write targets, cheap counts

### Open items

- 1-week taxi batch-vs-realtime validation for `realtime_rabbitmq`:
  - lock one fixed 1-week taxi dataset and schema
  - compute batch truth separately
  - replay the same data incrementally through RabbitMQ
  - compare final row counts, distinct row keys, and histogram bins for drift / duplicates / misses
  - include overlap, duplicate resend, and receiver-restart scenarios
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
  - `foxai_config.py`
  - `foxai_config.json`
- Registry contract: `kafka_consume_to_raw_domains.py` writes `raw_catalog.registry.raw_registry` — `bronze_from_raw_domains.py` must read the same table
- Current hardening status:
  - Fix 1 complete: shared config/env layer in place and consumed by pipeline files
  - Fix 2 complete: structured logging replaces `print()` in combined-domain Python files
  - Fix 3 complete: remaining DAG config cleanup completed
  - Fixes 4-5 pending

### B. Realtime histogram demo

- Kafka demo DAG id: `realtime_fare_amount_pipeline`
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

### C. Realtime RabbitMQ demo

- DAG id: `realtime_fare_amount_rabbitmq_pipeline`
- Separate validation DAG now active locally/on namenode: `realtime_validate`
- Validation purpose: compare whole-file `fare_amount` histogram output against row-replay `fare_amount` histogram output for the same source parquet
- Validation checkpoint status:
  - file mode path is working end-to-end with shallow output `demo/<snapshot>_file/fare_amount/...`
  - comparison JSON output is available for both modes once generated
  - Airflow monitor helper-server requirement was reconfirmed for task mutations
  - Airflow monitor live refresh logic was fixed locally so it keeps polling instead of going permanently idle after one run finishes
  - row-file mode is still architecturally inefficient and is the next approved fix target
- RabbitMQ broker: `192.168.100.60:5672`
- Queue: `daihai_local_test_1`
- DAG file: `/home/ubuntu/airflow/dags/realtime_fare_amount_rabbitmq_pipeline.py`
- Scripts: `/home/ubuntu/daihai_script/realtime_rabbitmq/`
- Receiver role: long-lived process outside Airflow; consumes queue, persists raw event to MinIO, triggers DAG via Airflow REST API
- Trigger behavior is intentionally automatic: once a message is received and persisted, receiver triggers the DAG without manual Airflow action
- Current trigger auth in receiver is hardcoded for now to Airflow API `http://192.168.100.66:8081/api/v1` with `admin/admin`
- Current temporary operating model: while Hoang is absent, our side manually simulates the upstream transmitter by sending file/row events through `rabbitmq_live_transmitter.py`
- Supported message types: `file`, `row`
- Event landing prefix: `demo/realtime_rabbitmq_fare_amount/event/`
- Persistent state: `demo/realtime_rabbitmq_fare_amount/state/`
- Calculation artifact: `demo/<snapshot>/fare_amount/calculation/summary.json`
- Chart output: `demo/<snapshot>/fare_amount/inrange.png`
- Current dedupe scope:
  - file-level dedupe by file event/hash
  - row-level dedupe by `event_id`
  - still requires validation against overlap/drift scenarios using a fixed 1-week taxi dataset
- Agreed next validation task:
  - compute offline batch truth for the same 1-week taxi slice
  - replay the identical data incrementally through RabbitMQ as file and/or row events
  - compare final state using exact row counts, distinct row keys, and histogram bin counts
  - run explicit duplicate, overlap, and receiver-restart scenarios to identify whether current dedupe is only transport-level or also business-row safe

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
- `realtime_rabbitmq/realtime_fare_amount_single_dag.py`
- `realtime_rabbitmq/realtime_fare_amount_rabbitmq_ingest_event.py`
- `realtime_rabbitmq/realtime_fare_amount_rabbitmq_calculation_job.py`
- `realtime_rabbitmq/realtime_fare_amount_histogram_job.py`
- `realtime_rabbitmq/rabbitmq_live_receiver.py`
- `realtime_rabbitmq/rabbitmq_live_transmitter.py`

### Platform / legacy

- `dag/` — legacy taxi pipeline
- `dag_combined_domains/` — combined-domain pipeline
- `silver_histograms_dag/` / `silver_sample_histogram/` — histogram jobs

---

## 5. Recently Completed

- `realtime_validate` file-mode hardening and deployment completed:
  - split file vs row state/output paths instead of one shared state
  - removed parquet decode/materialization from file ingest
  - changed file calculation to Spark-based `spark-submit` execution
  - added `comparison.json` artifact for validation output
  - added duplicate-rerun recovery when downstream artifacts are missing
  - reduced noisy task log output to compact summaries
  - fixed chart hangs caused by excessive raw-max bin/tick generation
  - changed `fare_amount` histogram bounds to use a two-stage IQR style upper bound
- Remaining realtime validation issue is row-file performance, not file-mode correctness

- Kafka-first realtime histogram demo — full flow built and verified
- Poller support for `.json`, `.csv`, `.parquet`, `.xml`
- Fixed bootstrap vs consumer-group offset behavior
- Retry-safe histogram gate logic
- Flattened demo output path to `demo/<snapshot>/fare_amount/...`
- Viewer path logic synced with flattened structure
- Reset helper for MinIO state + Kafka replay
- RabbitMQ realtime demo deployed and working on namenode:
  - queue `daihai_local_test_1`
  - receiver persists raw events to MinIO then auto-triggers Airflow DAG
  - DAG split into ingest → gate → calculation → chart
  - receiver/transmitter + DAG/scripts pushed and remote hashes verified
  - verified trigger auth currently uses hardcoded Airflow API base `http://192.168.100.66:8081/api/v1` and `admin/admin`
  - confirmed receiver restart replays unacked messages after failed trigger, which is expected RabbitMQ behavior
  - current ops mode is temporary manual transmitter simulation while Hoang is absent
- Fixed registry mismatch in combined-domain bronze read path
- Airflow monitor live refresh behavior verified working after refactor
- Combined-domain safe hardening pass, deployed to namenode:
  - shared config layer added via `dag_combined_domains/foxai_config.py` + `dag_combined_domains/foxai_config.json`
  - combined-domain DAG and jobs updated to consume shared config
  - structured logging applied across combined-domain Python files
  - remaining DAG config cleanup completed
  - remote deployment verified by matching local/remote `sha256`

---

## 6. Near-term / Backlog

### Near-term

- Refactor `realtime_validate` row-file mode with a chunked/batched replay design so row semantics stay row-level without sending one RabbitMQ message + one JSON state rewrite per row
- Then run the fixed 1-week taxi batch-vs-row validation and compare file-vs-row counts/bins for duplicates, misses, and drift
- Build and run the 1-week taxi batch-vs-realtime validation plan for `realtime_rabbitmq`
- Stabilize Kafka KRaft-only startup runbook on namenode
- Taxi 2025 forecasting (train on 2020–2024, evaluate vs actuals)
- Combined-domain fix 4: add explicit failure-context logging before re-raise
- Combined-domain fix 5: add small observability logs for phase start/end, write targets, cheap counts

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
- For `html/airflow_monitor.html` task actions, use actual Airflow REST endpoints:
  - `POST /api/v1/dags/{dag_id}/clearTaskInstances`
  - `POST /api/v1/dags/{dag_id}/updateTaskInstancesState`
- Edits stay targeted — no broad overwrites, no parallel copies of active scripts

---

## 8. Resume Checklist

1. Which system is active? Combined-domain pipeline or realtime histogram demo?
2. For realtime demo: confirm Kafka topic, consumer group, DAG id, and target runtime files
3. Read relevant active code before editing
4. After namenode push, verify remote file content
5. Update this file only when architecture or progress meaningfully changes
