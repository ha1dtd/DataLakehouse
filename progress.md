# progress.md

## How To Use This File (Agent Instructions)

- Read this file at session start, after any compact, and before resuming any task.
- Never rely on chat memory alone — always read current on-disk file state before editing.
- After every file edit, deploy, validation milestone, or root-cause investigation milestone, immediately update the relevant task block below.
- When a task is fully complete and deployed, move it to the Completed Archive section.
- Never delete a task — archive it. Never summarize away exact file states.
- Record exact remote artifact paths, snapshot ids, replay ids, and blocker/root-cause signals needed to resume without chat memory.
- `Current Phase` and `Next Exact Step` must be specific enough that a new session can continue the task immediately.
- If adding a new task, copy the Task Template at the bottom.

---

## Active Tasks

---

### Task 1 — realtime_rabbitmq 5-Day File-vs-Row Validation

**Goal:** Build the clarified validation behavior on `realtime_rabbitmq`: 5 rows mapped to 5 days, with file mode run once on the full file and row mode run incrementally one day at a time so the final row-mode day-5 chart matches the file-mode final chart.

**Mode:** Hotfix currently active.

**Current Phase:** Phase 4 — User verified on 2026-05-20 that the `realtime_rabbitmq` DAG worked after deployment. The row/file isolation fix is effective and the DAG no longer skips calculation/chart due to the earlier state collision. Explicit chart-parity confirmation for `row_day5` vs `file` is still not recorded in chat.

**Next Exact Step:**
If resuming this task, continue from the local `realtime_rabbitmq` refactor rather than the old `realtime_validate` path:
1. Read the current MinIO artifacts under `demo/file/fare_amount/` and `demo/row_day1` through `demo/row_day5`.
2. Confirm whether `row_day5` vs `file` chart parity was explicitly checked; if not, compare the final artifacts now instead of rerunning blindly.
3. Only rerun file/row events if the existing MinIO outputs are missing or parity has not yet been established.

**Files In Scope**

- `realtime_rabbitmq/realtime_rabbitmq.py`
  - status: done | verified: remote runtime
  - DAG gate now validates `mode` and `snapshot_label` from ingest summary before allowing calculation/chart

- `realtime_rabbitmq/realtime_fare_amount_rabbitmq_ingest_event.py`
  - status: done | verified: remote runtime
  - file mode keeps full-file ingest semantics
  - row mode now writes cumulative parquet state after each accepted row
  - ingest summary now records `mode` and fixed MinIO folder label: `file` or `row_dayN`
  - file mode and row mode write to separate MinIO state namespaces

- `realtime_rabbitmq/realtime_fare_amount_rabbitmq_calculation_job.py`
  - status: done | verified: remote runtime
  - reads file mode from JSON state and row mode from parquet state
  - writes shallow MinIO output directly under `demo/file/...` or `demo/row_dayN/...`

- `realtime_rabbitmq/realtime_fare_amount_histogram_job.py`
  - status: done | verified: remote runtime
  - chart rendering now follows the same fixed folder labels from calculation

- `realtime_rabbitmq/inbox/batch/fare_amount_5day_validation_batch.json`
  - status: done | verified: local
  - canonical 5-row file-mode input for this task

- `realtime_rabbitmq/inbox/rows/fare_amount_row_day1.json`
  - status: done | verified: local
  - row-mode day 1 input

- `realtime_rabbitmq/inbox/rows/fare_amount_row_day2.json`
  - status: done | verified: local
  - row-mode day 2 input

- `realtime_rabbitmq/inbox/rows/fare_amount_row_day3.json`
  - status: done | verified: local
  - row-mode day 3 input

- `realtime_rabbitmq/inbox/rows/fare_amount_row_day4.json`
  - status: done | verified: local
  - row-mode day 4 input

- `realtime_rabbitmq/inbox/rows/fare_amount_row_day5.json`
  - status: done | verified: local
  - row-mode day 5 input

**Current On-Disk Truth**

- The active implementation target is now `realtime_rabbitmq`, not `realtime_validate`.
- Local inbox samples under `realtime_rabbitmq/inbox/` now contain exactly one 5-row batch file and five single-row day files.
- The five row-day JSON inputs now use row-specific `event_id` values (`fare-demo-5day-row-000N`) so row-mode events are not deduped against the file-mode batch rows.
- Local `realtime_rabbitmq` refactor changes now do the following:
  - file mode keeps full-file ingest → calculate → chart
  - row mode appends one row per event and writes cumulative parquet state after each row
  - file mode and row mode now use separate MinIO state namespaces, so row-mode state no longer appends onto file-mode state
  - row/file outputs write to shallow fixed MinIO folders:
    - `demo/file/fare_amount/...`
    - `demo/row_day1/fare_amount/...`
    - `demo/row_day2/fare_amount/...`
    - `demo/row_day3/fare_amount/...`
    - `demo/row_day4/fare_amount/...`
    - `demo/row_day5/fare_amount/...`
- Namenode deploy is complete for the refactor:
  - DAG pushed to `/home/ubuntu/airflow/dags/realtime_rabbitmq.py`
  - scripts pushed to `/home/ubuntu/daihai_script/realtime_rabbitmq/`
  - local/remote `sha256` matched for DAG + ingest/calc/chart files
  - remote `pyarrow` availability confirmed (`23.0.1`)
  - Airflow CLI confirmed DAG presence: `realtime_rabbitmq`
- User confirmed on `2026-05-20` that the deployed `realtime_rabbitmq` DAG worked after the row-event-id change and the file-vs-row state isolation fix.
- The chat record does not yet explicitly say whether `demo/row_day5/fare_amount/...` was compared against `demo/file/fare_amount/...`; treat that comparison as still needing confirmation unless the user says it was already checked.

**Risks**

- Row-mode parquet write/read depends on `pyarrow` availability in the remote runtime.
- The main remaining uncertainty is not runtime stability but whether final `row_day5` and `file` parity was explicitly verified.
- Any future change must preserve the clarified comparison contract: 5 file/row day steps, 5 row charts, and day-5 parity with file mode.
- Any future refactor over 2 files must use the script-first approach from `rule.md`.

---

### Task 2 — Combined-Domain Safe-Hardening

**Goal:** Complete the agreed safe-fix sequence across combined-domain pipeline files.

**Mode:** Hotfix (each fix one file at a time)

**Current Phase:** Fixes 1-3 complete and deployed. Fixes 4-5 partially done locally, not deployed.

**Next Exact Step:**
Complete Fix 4 (explicit failure-context logging before re-raise) and Fix 5 (lightweight observability logs) in remaining files beyond `bronze_from_raw_domains.py`, then deploy.

**Fix Sequence & Status**

- Fix 1: Centralize config/env — complete + deployed
- Fix 2: Replace `print()` with structured logging — complete + deployed
- Fix 3: Finish shared-config cleanup — complete + deployed
- Fix 4: Add explicit failure-context logging before re-raise — partial, done in `bronze_from_raw_domains.py` only
- Fix 5: Add lightweight observability logs — partial, done in `bronze_from_raw_domains.py` only

**Files Remaining for Fixes 4-5**

- All files in `dag_combined_domains/` except `bronze_from_raw_domains.py`
- Agent must read each file before editing — do not assume current state

**Risks**

- Fix 4-5 are partially applied — all other files are still in original state
- Do not assume any file has been updated unless explicitly listed as complete above

---

### Task 4 — Packaging Baseline From Setup Scripts

**Goal:** Deliver a package/binary that installs and configures the platform based on `setup_namenode_v5.sh` and `setup_datanode.sh`, without bundling customer DAGs or customer job scripts.

**Mode:** Hotfix currently active for task-memory clarification only

**Current Phase:** Phase 1 — Scope clarified

**Next Exact Step:** If resuming this task, first inspect `setup_namenode_v5.sh` and `setup_datanode.sh`, then define the exact package boundary from those scripts only. Do not fold customer DAGs or customer job scripts into the base package.

**Files In Scope**

- `setup_namenode_v5.sh`
  - status: unchanged | verified: no
  - packaging baseline must be derived from this installer/configuration flow

- `setup_datanode.sh`
  - status: unchanged | verified: no
  - packaging baseline must be derived from this installer/configuration flow

**Current On-Disk Truth**

- Packaging requirement is now clarified:
  - base package/binary should install and configure everything from `setup_namenode_v5.sh` and `setup_datanode.sh`
  - base package should not include customer DAGs or customer scripts
  - including one example script is acceptable, but customer-specific script content stays outside the package
  - add-on direction remains valid: customer installs package binary first, then writes scripts, then runs

**Risks**

- Prior packaging understanding was too broad.
- Any future packaging work must keep platform bootstrap separate from customer pipeline authoring.

---

## Completed Archive

### Task 3 — Operator Documentation + Docs Folder Consolidation

**Goal:** Create a Vietnamese operator-facing `.docx` manual for the final-form Data Platform workflow and consolidate document files under one folder.

**Mode:** Hotfix

**Current Phase:** Complete

**Next Exact Step:** None unless user requests another doc edit/regeneration. If resuming doc work, start from the current generator and overwrite the existing `.docx` instead of creating parallel copies.

**Files In Scope**

- `docs/generate_operator_guide_vi.js`
  - status: done | verified: local
  - generator for the operator manual
  - current content reflects user-requested scope:
    - remove opening `Lưu ý phạm vi`
    - remove patchy HTML monitor operation section
    - section 4 rewritten to describe final-form pipeline generically
    - histogram kept as a short independent/currently-separate note

- `docs/Tai_lieu_huong_dan_van_hanh_Data_Platform.docx`
  - status: done | verified: local
  - generated successfully from the updated generator
  - current canonical operator manual output

- `Docs/`
  - status: done | verified: local
  - `outputs/` was renamed to `Docs/`
  - all root-level `.doc/.docx` files were moved into `Docs/`

**Current On-Disk Truth**

- Operator manual exists at `docs/Tai_lieu_huong_dan_van_hanh_Data_Platform.docx`.
- Current generator exists at `docs/generate_operator_guide_vi.js`.
- Existing document inventory consolidated under `Docs/`:
  - `BaoCao_FoxAI_Platform.docx`
  - `DataLakehouse_document.docx`
  - `FoxAI_Customer_Deployment_Plan.docx`
  - `FoxAI_Feature_Approach_Report.docx`
  - `FoxAI_Implementation_Plan_v2.docx`
  - `FoxAI_KeHoach_TrienKhai.docx`
  - `KeHoachTrienKhai.docx`
  - `Tai_lieu_huong_dan_van_hanh_Data_Platform.docx`
- No `.doc/.docx` files remain at repo root.
- Do not add the Superset/Spark Thrift runbook discussion as a tracked task here unless the user explicitly wants it treated as one.

**Risks**

- The useful operator-guide generator was preserved under `docs/`; disposable `.tmp_*` helpers and temp runtime were removed.
- The operator manual is meant to describe the finished production-facing workflow, not current experimental Kafka/RabbitMQ validation paths.

---

## Task Template (copy when adding new task)

### Task N — [Title]

**Goal:** [What are we trying to accomplish]

**Mode:** Hotfix / Refactor

**Current Phase:** [Phase N — description]

**Next Exact Step:** [Exact next action — specific enough that agent can act without asking]

**Files In Scope**

- `path/to/file.py`
  - status: pending / in progress / done | verified: local / remote / no
  - [what this file does in this task]
  - [any constraints or risks specific to this file]

**Current On-Disk Truth**

- [file]: [exact current state — be specific, no summaries]

**Risks**

- [anything uncertain or worth flagging]

---

## Last Updated

2026-05-19T07:37:13Z — Added completed archive entry for operator documentation + `Docs/` consolidation. No Superset setup discussion was added as a tracked task. Task 1 and Task 2 status unchanged.
2026-05-19T08:52:52Z — Updated task memory after clarification from new supervisor input. Task 1 was re-scoped from the older large replay/chunk-state validation path to the actual 5-row / 5-day file-vs-row validation requirement. Added Task 4 to track packaging scope based on `setup_namenode_v5.sh` and `setup_datanode.sh`, with platform bootstrap separated from customer scripts/DAGs.
2026-05-19T09:40:18Z — Refactor Mode patch set for Task 1 was deployed to `realtime_rabbitmq`. Local inbox samples were replaced with one 5-row batch file plus five one-row day files. DAG/ingest/calc/chart files now write fixed MinIO folder labels `file` and `row_day1` through `row_day5`, and row mode materializes parquet state after each accepted row. Namenode deploy completed with matching local/remote `sha256`, remote `pyarrow` confirmed (`23.0.1`), and Airflow CLI confirmed DAG presence: `realtime_rabbitmq`. Runtime validation still pending.
2026-05-19T09:40:18Z — Updated the five `realtime_rabbitmq` row-day inbox files so their `event_id` values are row-specific (`fare-demo-5day-row-0001` ... `0005`) instead of matching the file-batch row IDs. This avoids row-mode duplicate suppression after file mode while keeping the same business-row values for chart comparison. Updated local files were pushed to the namenode inbox path and verified by content.
2026-05-19T09:40:18Z — Fixed file-vs-row state collision in `realtime_rabbitmq`: ingest/calculation now use separate MinIO state keys for file mode and row mode (`.../file/...` vs `.../row/...`), so row-mode charts no longer build on top of file-mode state. Updated ingest/calculation files were pushed to the namenode and verified by matching local/remote `sha256`.
2026-05-20T01:44:50Z — User confirmed the deployed `realtime_rabbitmq` DAG worked after the row-event-id change and the file-vs-row state isolation fix. Treat runtime execution as verified on namenode. The chat does not yet explicitly record whether `demo/row_day5/fare_amount/...` was compared against `demo/file/fare_amount/...`, so keep that as the next precise validation check unless the user says it was already done.
