# Combined Domain DAG - 1 Week Production Readiness Plan

## 0) Current State (Based on Current Working Version)

This plan is based on what is already working now:

- End-to-end Airflow DAG exists: ingest request enqueue -> Kafka consume -> Bronze -> Silver -> Gold.
- Domain-driven behavior is implemented via `domain_registry_v2.json`.
- Multi-domain support is already present (taxi and non-taxi branches).
- Schema variability handling exists in Silver (`read_and_merge_varying_schemas` with `unionByName(..., allowMissingColumns=True)`).
- Basic quality filters exist for core taxi fields.
- Gold KPIs/summary tables are generated and written successfully.

What is **not yet production-grade**: governance controls, idempotency guarantees, observability, security hardening, packaging/config standardization, and operations playbooks.

### Fixed Platform Versions (Must Not Change)

From `setup_namenode_v5.sh`, these are fixed and must remain exact for compatibility:

- Ubuntu: 22.04 LTS
- Java runtimes:
  - Temurin JDK 11 (`/usr/lib/jvm/temurin-11-jdk-amd64`) for Hadoop/Spark runtime env
  - Temurin JDK 17 (`/usr/lib/jvm/temurin-17-jdk-amd64`) installed on hosts
- Hadoop: 3.3.6
- Spark: 3.5.8 (build: `spark-3.5.8-bin-hadoop3`)
- Spark SQL Kafka package line in DAGs/scripts: 3.5.1 artifact family (`spark-sql-kafka-0-10_2.12:3.5.1`, `kafka-clients:3.5.1`)
- Iceberg runtime package line in DAGs/scripts: `iceberg-spark-runtime-3.5_2.12:1.4.3`
- Hadoop AWS package line in DAGs/scripts: `hadoop-aws:3.3.4`
- AWS Java SDK bundle line in DAGs/scripts: `aws-java-sdk-bundle:1.12.262`
- Scala binary line implied by packages: 2.12

Rules for this project template:

- Do not upgrade/downgrade these versions during customer onboarding unless doing a dedicated compatibility project.
- New customer deployments must start from this locked stack first, then tune configs/business mappings only.
- If a customer requires different versions, treat that as a separate migration track with regression testing.

---

## 1) Production Goal (Template for Multiple Customers)

Build a standard, reusable pipeline template that can be deployed on different on-prem customer environments with minimal edits:

- customer-specific infra values configurable (server, object store, Kafka, credentials, paths)
- customer-specific source definitions configurable (domains/topics/files)
- customer-specific business rules configurable (silver checks, gold KPIs)
- stable deployment behavior, monitoring, and rollback procedures

Aligned L0 scope (supervisor-verified):

- Support mixed incoming types: long CSV, short JSON/CDC payloads, image/audio/video byte streams.
- Classify by MIME (and/or topic routing) into structured vs unstructured paths.
- Keep ingestion mode support: Kafka, Debezium CDC, and manual ingestion trigger.
- Preserve metadata completeness at each layer: schema, source, ingestion time, size, domain, technical attributes.
- Ensure raw -> parquet_bronze compaction supports interval trigger and manual DAG trigger.
- Keep schema evolution/version timeline governed through Iceberg metadata.

---

## 2) Gaps Blocking Production Today

## A. Configuration & Portability

- Hardcoded infra values are still in code (endpoints, keys, topics, script paths).
- No strict dev/staging/prod config separation.
- No single “customer config pack” pattern yet.
- No standardized mapping template for L0 folder hierarchy (`raw_bronze/date/topic`, `parquet_bronze/date/topic`, `silver/domain`).
- Manual override config for category/routing is not formalized yet.

## B. Data Contracts & Quality Governance

- Quality checks are embedded but not centrally declared/versioned.
- Missing threshold-based pass/fail policy (e.g., max null %, max bad records %).
- No quarantine table/path for rejected records.
- MIME/type classification policy is not codified as a contract (structured vs unstructured pathing rules).
- Metadata contract is incomplete for mandatory technical fields (schema, source, size, ingestion time, domain).

## C. Idempotency & Replay Safety

- Replace-style writes are easy but risky for reruns/backfills.
- No explicit run watermark strategy and deterministic dedup policy across all domains.

## D. Observability & Alerting

- No unified metrics output by step/domain (in rows, out rows, dropped rows, duration).
- No production alert rules for lag/failure/data freshness.

## E. Security & Secrets

- Credentials currently exposed in plain code patterns.
- Missing secret externalization and minimum-permission service account model.

## F. Performance for Real Data Scale

- Spark defaults are static and not customer/profile aware.
- No controlled compaction/file-size policy after heavy writes.
- No systematic skew/partition diagnostics process.

## G. Release & Operations

- No formal runbook for incident handling and rollback.
- No standard onboarding checklist for new customer deployments.

---

## 3) 1-Week Execution Plan (Day Goals + Tracking)

Use this section as the active tracker. Update status daily.

Status keys: `NOT_STARTED` | `IN_PROGRESS` | `BLOCKED` | `DONE`

| Day   | Goal                              | Status      | Exit Criteria                                                 |
| ----- | --------------------------------- | ----------- | ------------------------------------------------------------- |
| Day 1 | Config externalization baseline   | NOT_STARTED | Same DAG run passes on at least 2 profiles without code edits |
| Day 2 | Data quality + metadata contracts | NOT_STARTED | Quarantine + threshold policy active and auditable            |
| Day 3 | Idempotency + replay safety       | NOT_STARTED | Rerun same window yields stable counts, no duplication        |
| Day 4 | Observability + SLA signals       | NOT_STARTED | Stage metrics + freshness/reject alerts visible               |
| Day 5 | Security + secrets hardening      | NOT_STARTED | No plaintext secrets in runtime path                          |
| Day 6 | Performance tuning pass           | NOT_STARTED | Measured runtime improvement with no correctness regression   |
| Day 7 | Production package + runbook      | NOT_STARTED | New customer dry run succeeds with config-driven onboarding   |

### Day 1 - Config Externalization Baseline

Goal:

- Remove hardcoded infra/runtime values from execution logic.

Tasks:

- Move all infra and runtime vars to external config (`.json`/env profile).
- Keep code reading config only (no direct hardcoded endpoints/keys in execution path).
- Define profile structure: `dev`, `staging`, `prod`, plus `customer_x`.
- Add explicit L0 routing config blocks for:
  - topic -> domain/topic folder mapping,
  - MIME -> structured/unstructured classification,
  - raw/parquet/silver root path conventions.

Exit criteria:

- Same DAG runs with only config changes (no code edits) across at least 2 profiles.
- L0 routing behavior is changeable via config only.

Evidence to record:

- Profile files used.
- DAG run IDs for both profiles.

### Day 2 - Data Quality Contract Layer

Goal:

- Make data quality and metadata rules declarative and enforceable.

Tasks:

- Add declarative quality rules per dataset/domain (required cols, non-negative metrics, timestamp sanity).
- Add thresholds (warn/fail) and quarantine output for invalid rows.
- Emit summary metrics per rule.
- Add metadata contract enforcement for required fields in bronze/parquet_bronze/silver (schema, source, ingestion_time, domain, size where applicable).

Exit criteria:

- Silver step can fail fast on severe quality breach and still preserve bad-row evidence.
- Required metadata fields are always present and auditable.

Evidence to record:

- Quality rule config snapshot.
- Quarantine sample path/table.
- Failed-rule run output.

### Day 3 - Idempotency, Replay, and Backfill Safety

Goal:

- Guarantee deterministic outputs during reruns/backfills.

Tasks:

- Define per-domain primary keys / dedup keys.
- Add deterministic dedup and rerun-safe write behavior.
- Define backfill window parameters and watermark handling.

Exit criteria:

- Rerunning same batch/window does not duplicate output and produces stable row counts.

Evidence to record:

- Before/after row-count comparison.
- Replay test run IDs.

### Day 4 - Observability and SLA Signals

Goal:

- Provide operational visibility for run health and freshness.

Tasks:

- Add run metrics table/log schema:
  - domain, dataset, step, input_count, output_count, reject_count, duration_sec, run_id.
- Add basic alert points (task failed, freshness stale, reject ratio high).
- Add L0 trace fields for file/object lineage where possible (topic, object path, ingest mode: kafka/debezium/manual).

Exit criteria:

- Each DAG run yields auditable metrics across all stages.
- L0 lineage can trace from raw object/topic to downstream table batch.

Evidence to record:

- Metrics table sample rows.
- Alert trigger test evidence.

### Day 5 - Security & Secrets Hardening

Goal:

- Remove plaintext secrets and enforce least-privilege runtime access.

Tasks:

- Remove static credentials from runtime code path.
- Use environment/secret mount/injected vars only.
- Validate least-privilege access for storage/Kafka/metastore operations.

Exit criteria:

- No plaintext secrets in deployed DAG/scripts.

Evidence to record:

- Secret source mapping (env/secret mount).
- Access policy checklist result.

### Day 6 - Performance Tuning Pass

Goal:

- Improve runtime/throughput without changing business correctness.

Tasks:

- Profile largest domain path and optimize:
  - partitioning/shuffle settings,
  - file size / write distribution,
  - optional compaction action.
- Capture target throughput and baseline runtime report.

Exit criteria:

- Demonstrable runtime improvement with no correctness regression.

Evidence to record:

- Baseline vs tuned runtime metrics.
- Data validation comparison summary.

### Day 7 - Production Package + Runbook

Goal:

- Finalize customer-ready delivery package and prove onboarding flow.

Tasks:

- Final “customer-ready template” package:
  - config samples,
  - onboarding checklist,
  - runbook (failures, replay, rollback),
  - deployment steps.
- Dry run using a simulated new customer profile.

Exit criteria:

- New profile onboarding can be done by config + minor mapping adjustments only.

Evidence to record:

- Dry-run profile + run ID.
- Final package artifact list.

---

## 4) Definition of Done (Production-Ready Template)

Pipeline is considered production-ready when:

- A new customer can be onboarded mainly via config + mapping rules.
- Quality contracts are explicit, versioned, and enforceable.
- Reruns/backfills are deterministic and safe.
- Metrics and alerts are available for operations.
- Secrets are externalized and access is least-privilege.
- Runbook exists for failure/recovery/rollback.

---

## 5) Out of Scope for This Week (Future Work)

Planned later (not in this week):

- fully automated bootstrap `.sh` to collect customer inputs and auto-generate configs/scripts
- advanced metadata UI/self-service controls for business users
- deep cost-based auto-tuning and adaptive optimization policies
- extended security/compliance policy pack once customer legal/regulatory constraints are finalized

---

## 6) Immediate Next Step (Start Tomorrow)

1. Freeze current working DAG as baseline tag/snapshot.
2. Implement config externalization first (Day 1) without changing business logic.
3. Validate end-to-end run parity after config refactor.
