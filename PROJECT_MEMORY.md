# FoxAI Project Memory

## Purpose
Single source of truth for AI context. Future sessions read this file to get immediately up to speed.

---

## 1. Current Architecture (On-Prem VMs)
- **Deployment:** 1 NameNode, 5 DataNodes (company network `192.168.100.x`)
  - Existing DNs: `192.168.100.61`, `192.168.100.62`
  - New DNs: `192.168.100.63`, `192.168.100.64`, `192.168.100.65`
- **Node specs:** 16GB RAM, 16 cores each
- **YARN effective limits per node:** ~`13312 MB` memory, `14 vcores` (container scheduling constraints)
- **Storage:** MinIO S3A endpoint `http://192.168.100.66:9001` (`admin` / `12345678`)
- **HDFS role:** cluster services + temp paths only (not primary data lake storage)
- **Table format/catalog:** Apache Iceberg + HadoopCatalog (`type=hadoop`)
- **Warehouse paths:** use `lakehouse/` prefix inside buckets (example: `s3a://silver/lakehouse/`, `s3a://gold/lakehouse/`)
- **Orchestration:** Airflow on NameNode, DAG id `spark_minio_transform_pipeline`

## 2. Active Services & Ports
| Service               | Port   | Access / Status |
|-----------------------|--------|-----------------|
| HDFS NameNode         | 9000   | Internal |
| YARN ResourceManager  | 8088   | Web UI |
| MinIO API             | 9001   | Web UI |
| Airflow Webserver     | 8080   | Web UI |
| Spark Thrift Server   | 10000  | Optional / not current optimization scope |
| Apache Superset       | 8084   | Optional / not current optimization scope |
| Trino                 | 8083   | Installed but sidelined |

## 3. Data Pipeline (Medallion)
- **Bronze:** raw taxi parquet data uploaded to MinIO (`bronze` bucket)
- **Silver (`silver.py`, renamed from `bronze_to_silver.py` on 2026-04-21):** schema normalization + cleaning + enrichment into Iceberg silver tables (`yellow_taxi`, `green_taxi`, `fhv_trip`, `fhvhv_trip`)
- **Gold (`gold.py`, renamed from `silver_to_gold.py` on 2026-04-21):** analytical outputs:
  - `gold_catalog.default.yellow_taxi_tips`
  - `gold_catalog.default.yellow_taxi_performance`
  - `gold_catalog.default.yellow_taxi_financials`
  - `gold_catalog.default.yellow_taxi_classifications`
  - `gold_catalog.default.green_taxi_tips`
  - `gold_catalog.default.green_taxi_performance`
  - `gold_catalog.default.green_taxi_financials`
  - `gold_catalog.default.green_taxi_classifications`
  - `gold_catalog.default.fhv_trip_summary`
  - `gold_catalog.default.fhvhv_trip_summary`

## 4. Key Files
- `setup_namenode_v5.sh`: **current primary** NameNode bootstrap for 2-existing + 3-new DataNode expansion (idempotent style)
- `setup_datanode.sh`: manual setup script run on each new DataNode after NameNode setup
- `setup_namenode_v4.sh`: previous baseline script (kept for reference)
- `dag.py`: Airflow DAG orchestrator (`spark_minio_transform_pipeline`)
- `silver.py`: Bronze→Silver ETL (renamed from `bronze_to_silver.py` on 2026-04-21)
- `gold.py`: Silver→Gold ETL (renamed from `silver_to_gold.py` on 2026-04-21)
- `read_bronze.py`: bronze schema/sample inspection utility
- `HDSD_Pipeline.md`: operational guide

## 5. Architectural Decisions
- **Dropped Hive Metastore:** avoided compatibility/path conflicts with MinIO + Spark per-catalog setup
- **Spark Thrift preferred over Trino for Iceberg compatibility**, but BI-layer work is currently deprioritized
- **No YAML DAGs:** keep Python Airflow DAGs
- **Performance decision rule:** do bottleneck-first estimation before promising speedups (config tuning helps only when not already I/O/network bound)

## 6. Next Actions
- [DONE] Scale cluster from 2 → 5 DataNodes via `setup_namenode_v5.sh` + `setup_datanode.sh`
- [DONE] Apply Spark tuning in DAG (`AQE on`, executors `10`, executor memory `6G`, shuffle partitions `200`)
- [DONE] Optimize `bronze_to_silver.py` schema detection (`limit(1)` instead of full-read)
- [DONE] Re-benchmark after reconfig: observed runtime did **not** improve materially for current workload
- [DONE] Architecture alignment completed for "combined pipeline" direction: keep old taxi pipeline stability traits + add new generic Kafka/registry/error-handling capabilities without modifying old taxi pipeline directly
- [DONE] Clarified core ingestion principles for current design: hybrid metadata (control hardcoded + runtime derived), Kafka role boundaries, Debezium/CDC role, and stream-to-bucket industry pattern
- [NEXT] Draft and lock a phased "Lego" upgrade plan (step-by-step, gate-by-gate) from current 2 pipelines to target bank-grade final structure
- [NEXT] Define hard production gates per phase: idempotency/replay safety, schema evolution contract, DLQ/quarantine behavior, and rollback criteria
- [NEXT] Add evidence-based benchmark table (per-stage read/write volume, shuffle read/write, task spill, throughput)
- [NEXT] Parameterize `dag.py` (remove hardcoded MinIO endpoint + credentials)
- [NEXT] Add practical Iceberg partition strategy for high-volume tables
- [NEXT] If strict runtime target is still required, prioritize infra/I/O improvements (network bandwidth, MinIO throughput, write parallelism strategy)
- [NEXT] Implement error bucket feature in bronze-to-silver: move invalid/mismatched-schema files to separate "error" bucket for tracking/debugging
- [NEXT] Build prediction model: use 2024 and backward silver data to predict 2025 outcomes, then compare with actual 2025 data
- [NEXT] Add data shuffle to histogram sample job: shuffle via `rand()` to ensure representative sampling across source files (avoid locality bias where majority data appears in first 10% of parquets)

---

**Session log policy: entries below must always be ordered from oldest → newest.**

## Session Log Archive
<details>
<summary>Expand session logs</summary>

### 2026-04-01
- Consolidated project context into `PROJECT_MEMORY.md`
- Confirmed architecture direction: Spark + YARN + MinIO + Airflow + Iceberg

### 2026-04-06
- Drafted metadata/domain-driven pipeline plan
- Agreed modular scripts (`metadata_ingest.py`, `categorize.py`, `move_to_domain.py`, `bronze_to_silver.py`, `silver_to_gold.py`)

### 2026-04-14
- Confirmed migration away from Google Cloud toward company on-prem VMs
- Drafted `HDSD_Pipeline.md` (Vietnamese operational guide)

### 2026-04-15
- Abandoned Hive Metastore approach for this stack
- Restored Spark jobs to native Iceberg HadoopCatalog on MinIO
- Added `lakehouse/` warehouse prefix for stable Spark Thrift reads
- Set up Spark Thrift Server (10000) and Superset (8084)
- Created consolidated `setup_namenode_v4.sh`

### 2026-04-17
- Fixed mixed-schema merge failure (`INT` vs `DOUBLE`) in `bronze_to_silver.py` using dynamic schema reconcile + `unionByName`
- Added `.cache()` + `.count()` + `.unpersist()` pattern in `silver_to_gold.py` for repeated branch reads
- Diagnosed YARN memory under-allocation and tuned NodeManager/Scheduler memory+vcores
- Updated `setup_namenode_v4.sh` to support 5 DataNodes
- Noted baseline runtime pain on old config (heavy spill / long bronze run)

### 2026-04-20
- Full-day summary: performance review, cluster v5 preparation, code/config fixes, deployment prep, and post-reconfig validation
- Identified bottlenecks: machine/network constraints + Spark config gaps + one logic inefficiency
- Applied DAG tuning (`AQE`, 10 executors, 6G executor memory, 200 shuffle partitions)
- Applied `bronze_to_silver.py` optimization (`detect_yellow_schema()` uses `.limit(1)`)
- Fixed critical DAG shell issues (heredoc closer indentation + missing backslash on `spark.sql.extensions` line)
- Backed up production scripts on namenode (`dag.py`, `bronze_to_silver.py`, `silver_to_gold.py`)
- Confirmed `silver_to_gold.py` logic unchanged in this round
- Post-reconfig validation showed runtime stayed close to pre-change behavior for current workload
- Updated conclusion: pipeline currently appears primarily I/O/network bound (MinIO read/write ceiling), so config tuning alone gives limited wall-clock gain
- Process lesson recorded: future estimates must include hard bottleneck ceilings before quoting expected speedup

### 2026-04-21
- Pulled 3 histogram files from namenode to local (`silver_histograms_sample_dag.py`, `silver_histograms_full_dag.py`, `silver_histograms.py`)
- Deleted wrongly created local files (`generate_histograms.py`, `dag_histogram_sample.py`, `dag_histogram_full.py`)
- Read Chapters 1 & 2 of *Hands-On Machine Learning* (Géron, 3rd Ed.) covering ML landscape/types and end-to-end ML project workflow
- Created local sample job with proper shuffle step: `/Users/daihai/Documents/Code/FoxAI/silver_histograms_sample.py` using `rand()` to ensure every row has equal sampling probability
- Clarified shuffle concept with Dan: confirmed data-level clustering interpretation (majority data in first 10% of parquets causes wrong trend) over node-level partition interpretation
- Confirmed trap: `df.describe()` runs on full table in sample DAG mode, not on sampled data — user must refactor to describe only sampled data if needed
- Verified local and namenode ETL scripts matched byte-for-byte before rename
- Renamed ETL scripts on both local and namenode: `bronze_to_silver.py` → `silver.py`, `silver_to_gold.py` → `gold.py`
- Updated Airflow DAG on both local and namenode to use the renamed script paths (`{SCRIPT_BASE}/silver.py`, `{SCRIPT_BASE}/gold.py`)

### 2026-04-22
- Reviewed architecture progression artifacts (`dtlver1.jpg`, `dtlver2.jpg`, `dtlver3.jpg`) and aligned on target direction represented by version 3.
- Confirmed strategic objective: build a combined ingestion framework for bank-grade use that preserves old taxi pipeline stability while adding generic Kafka/registry/error-handling capabilities.
- Locked scope rule with Dan: do not modify old taxi pipeline directly; validate hardening through the new combined path.
- Clarified foundational concepts for alignment: hybrid metadata model (minimal control fields hardcoded + runtime-derived metadata), Kafka as transport/event log (not business-metadata inference), and Debezium/CDC role in source capture.
- Recorded readiness assessment: architecture is close, but production hardening proof is still pending (idempotency, schema evolution contracts, replay safety, DLQ/quarantine behavior, load/restart/failure tests, and observability gates).
- Agreed next collaboration mode: produce a precise, step-by-step "Lego" upgrade roadmap with strict phase gates to minimize context switching and avoid breakage.

</details>
