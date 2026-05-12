# FoxAI Pipeline Progress

## Accomplished (The Foundation)

1. **Unified Medallion Architecture**:
   Transitioned from a hardcoded single-domain script to a unified pipeline (`dag_combined_domains`) processing multiple domains (taxi, hr, finance, marketing) via a central registry.

2. **Decoupled Ingestion via Kafka**:
   Replaced direct file reads with a Kafka-driven ingestion model (`kafka_enqueue_ingest_domains.py` -> `kafka_consume_to_raw_domains.py`), standardizing data entry.

3. **Data Registry & Tracking**:
   Implemented `domain_registry_v2.json` and control registry logic to audit processed files, locations, and success/failure states.

4. **Modern Data Lakehouse Stack**:
   Integrated Spark with Apache Iceberg and MinIO S3, utilizing `HadoopCatalog` to eliminate the heavy Hive Metastore dependency.

5. **Automated Visualization / Observability**:
   Built the `html_histograms` viewer with Airflow API integration to automatically poll MinIO and render Spark-generated charts.

6. **Clear Operational Guidelines**:
   Established `PROJECT_MEMORY.md` and `HDSD_Pipeline.md` to document and enforce rules of engagement.

## Missing Optimizations (Production Readiness)

1. **Iceberg Schema Evolution & Upserts**:
   Replace `.createOrReplace()` and `.mode("overwrite")` with `MERGE INTO` (upserts) or `.append()` with `mergeSchema=true` to handle daily data and schema changes.

2. **Robust Error Handling (Dead Letter Queue - DLQ)**: Implement a DLQ mechanism to quarantine bad records to an `error/` path instead of failing the entire Spark job.

3. **Idempotency and Deduplication**:
   Ensure Spark writes are idempotent so pipeline retries or replays do not duplicate records.

4. **Partitioning Strategy**:
   Partition Iceberg tables (e.g., by `days(ingest_ts)` or `domain`) to prevent slow full table scans at scale.

5. **Automated Testing / Data Quality Gates**:
   Add quality checks before promoting data between Bronze, Silver, and Gold layers.

6. **Config Parameterization**:
   Dynamically inject Spark configurations (memory, cores, shuffle partitions) via Airflow variables instead of using hardcoded `SPARK_COMMON` strings.

7. **Data Retention & Compaction (Iceberg Maintenance)**: Schedule automated `RewriteDataFiles` (compaction) and `ExpireSnapshots` jobs to maintain query performance.

8. **Monitoring & Alerting**:
   Add proactive alerting (Slack/Email) for DAG failures, Kafka lag spikes, or critical disk space levels.

9. **Security & Access Control**: Securely inject MinIO credentials via secret managers and define table-level access controls for the Gold layer.
