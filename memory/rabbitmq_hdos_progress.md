# RabbitMQ HDOS Progress

## Checkpoint — 2026-05-18

### Current status

- Active validation DAG: `realtime_validate`
- Goal: compare `fare_amount` histogram correctness between:
  - file mode: whole silver parquet as batch truth
  - row-file mode: same parquet replayed as row-level semantics
- File mode checkpoint is now considered structurally correct and deployed on namenode.

### What is working now

- File ingest is pointer-only and no longer decodes/materializes parquet in Python.
- File calculation runs via Spark `spark-submit`.
- Shallow output path remains compatible with the existing HTML viewer:
  - `demo/<snapshot>_file/fare_amount/...`
  - `demo/<snapshot>_row/fare_amount/...`
- Validation artifacts now include:
  - `calculation/summary.json`
  - `comparison.json`
  - `summary.json`
  - `inrange.png`
- Duplicate reruns do not skip if downstream artifacts are missing.
- Calculation/chart task logs were reduced to compact summaries.
- Chart scaling was corrected twice:
  - first to stop raw-max bin explosion and chart hangs
  - then to use a two-stage IQR style upper bound like the sample histogram job

### Current known problem

- Row-file mode is still inefficient and is the next fix target.
- Current row-file weakness in repo/runtime shape:
  - `rabbitmq_row_file_transmitter.py` reads parquet in pandas and materializes full rows in memory
  - row replay is still modeled as per-row messaging/state updates
  - row ingest reloads and rewrites large JSON state
  - row calculation still reads row-state JSON in Python
- Result: file mode is acceptable, row-file mode is not yet production-worthy for large replay volume.

### Approved next direction

- Use Approach A for row-file mode:
  - keep user-visible semantics as file-vs-row validation
  - but transport row-file replay in chunked/batched form instead of one physical message per row
  - preserve row-level dedupe/validation semantics inside the chunked replay path
- Supervisor goal remains data-correctness comparison, not literal broker stress by millions of single-row messages.

### Resume notes

- Re-read `rule.md`, `project.md`, `logs.md`, and this file before starting the row-file refactor.
- Do not regress file mode while refactoring row-file mode.
- Validate that the final row-file design still supports detecting:
  - duplicate rows
  - missing rows
  - drift between batch truth and row replay
