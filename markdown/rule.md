# FoxAI Agent Rules

## Startup Read Order

- Before any fresh workday/session start, read files in this exact order:
  1. `markdown/rule.md`
  2. `markdown/project.md`
  3. `markdown/logs.md`
- If resuming an in-progress task or any task that may have been compacted, read files in this exact order:
  1. `markdown/rule.md`
  2. `markdown/project.md`
  3. `markdown/logs.md`
  4. `markdown/progress.md`
- If user only says "read rule.md" outside of session start, read only `markdown/rule.md` — skip the other three.
- Remove/ignore any older startup instruction that conflicts with this order.

---

## progress Filename Rule

- The session state file is exactly `markdown/progress.md`.
- Do not infer filename variants such as `.MD` or other casing.
- If the file is missing, say so explicitly instead of silently substituting another file.

---

## Project Memory Maintenance

- In `project.md`, only active / in-progress work should stay in active task sections such as:
  - `Current Active Work`
  - `Active Systems`
  - `Near-term / Backlog`
- Completed work should be moved down into `project.md` section `## 5. Last Updated`.
- When the user explicitly says a task is completed, move that task out of the active section and record it under `## 5. Last Updated` instead of leaving it mixed with active work.

---

## Non-negotiables

- Make targeted edits only. No parallel copies (new/final/fixed).
- Read the exact active code before editing anything.
- Inspect the whole flow before patching multi-component behavior.
- Never claim fixed without a real verification path.
- After edits, check for editor errors.

---

## Deploy Rules

- Namenode access: `ssh nn` (assume shell is already on namenode when giving commands)
- DAG files → `/home/ubuntu/airflow/dags` — flat, never inside a subfolder
- Job scripts → `/home/ubuntu/daihai_script` — can be nested in subfolders
- Never push HTML files to namenode — served from local HTTP server only.
- When pushing to namenode, push and verify in one command to save token/time.
- After pushing a DAG file, confirm it appears in Airflow via `airflow dags list | grep <dag_id>` before confirming success.

---

## Architecture Guardrails

### Storage

- MinIO is the only persistent storage layer. All data goes to `s3a://` paths.
- HDFS for temporary compute only — never for business data.
- Forbidden: `hdfs://` business tables, local Parquet/CSV persistence.

### Table Format

- Iceberg is the only table format for managed datasets.
- Forbidden: direct Parquet overwrites on Iceberg locations, manual file manipulation inside Iceberg directories, mixing unmanaged Parquet with Iceberg tables.

### Data Layers (Medallion — strict)

- Bronze: raw, append-only, immutable. No business logic.
- Silver: cleaned, deterministic transformations only.
- Gold: analytics-ready, documented business meaning.
- Forbidden: skipping layers, mutating Bronze, mixing business logic into ingestion.

---

## Pipeline Rules

### Idempotency (required on every pipeline)

- Re-running must never create duplicates, corrupt tables, or produce non-deterministic results.
- Define: merge/upsert strategy, overwrite semantics, checkpoint handling.
- Every pipeline must answer: what happens on rerun? On partial failure? On duplicate input?

### Failure Handling

- Explicit exception handling and logging on every pipeline.
- No silent exception swallowing, no partial writes without recovery strategy.
- All failures must be logged with context.

### Schema Management

- Schema changes must be explicit: define backward compatibility, downstream impact, migration strategy.
- Forbidden: implicit schema drift, blind schema overwrite, auto-generated uncontrolled columns.

---

## Spark Rules

- One job = one clear responsibility.
- Forbidden: `collect()` on large datasets, Cartesian joins without justification, blind `cache()`, unbounded repartition.
- Every large transformation must consider: shuffle cost, memory impact, partition skew, file size.

---

## Airflow Rules

- Airflow is orchestration only — DAGs trigger Spark jobs, nothing more.
- Forbidden: business logic or heavy transformations inside DAGs or PythonOperator.
- All tasks must be independently retryable as a design goal — note: existing pipelines
  may not yet fully satisfy this. Treat as a guardrail for new work, not a claim about current code.

---

## Config Rules

- No hardcoded credentials, endpoints, or environment-specific logic scattered in code.
- Use centralized config with environment separation (dev / staging / prod).

---

## Observability (required on every pipeline)

- Log: input row count, output row count, execution duration, failed record count, schema info
  — where count operations are cheap and reasonable. Skip exact counts if prohibitively expensive;
  use estimates or sampling instead and note the tradeoff.
- Validate: null rates, duplicate rates, schema consistency, partition validity.

---

## Code Quality

- Modular functions, reusable utilities, explicit naming, deterministic behavior.
- Forbidden: monolithic scripts, duplicated logic, magic constants, hidden side effects.

---

## AI Code Generation Rules

Before generating any code, state the following — full detail for refactor/architecture work,
brief acknowledgment for small isolated hotfixes:

1. Architecture impact
2. Idempotency strategy
3. Failure handling approach
4. Schema handling
5. Scalability concerns

Never:

- Introduce new storage layers
- Bypass Iceberg
- Write unmanaged Parquet directly
- Hardcode credentials
- Create hidden side effects
- Generate code that cannot be verified

Priority order for all output:

1. Correctness
2. Data safety
3. Operational safety
4. Scalability
5. Developer convenience

Default preference: explicit over implicit · deterministic over clever · safe over fast · observable over opaque.

---

## Pre-code Checklist (mandatory)

- Read the exact file before editing
- State the idempotency strategy for this change
- State what happens on rerun
- Confirm this touches MinIO/Iceberg correctly

---

## Edit Modes

### Hotfix Mode (default)

- One file at a time, stop and confirm between each file.
- Targeted edits only — touch nothing outside the immediate problem.
- When in doubt, default to this mode.

### Refactor Mode (for cleanup, restructure, architecture work)

- Inspect the full affected flow before touching anything.
- Propose a scoped plan: which files, what changes, why — wait for approval before starting.
- Multiple related files may be edited in one pass if they implement one logical fix, within approved scope only.
- Prefer minimal coherent change-set over per-file patchwork.
- Validate end-to-end behavior before closing the refactor.
- Never expand scope mid-refactor without explicit confirmation.

### Mode Switching

- Default is always Hotfix Mode.
- Refactor Mode activates only when user explicitly says "refactor mode" or "clean this up properly".
- Agent must confirm which mode is active at the start of any multi-file task.

---

## Systemic Thinking

- Before fixing a problem locally, assess whether the root cause is architectural.
- If a local patch would mask a deeper structural issue, say so explicitly before patching.
- Propose the systemic fix alongside the local fix — let the user decide which to take.

---

## Proactive Architecture Improvement

- If you notice a pattern across files that violates the architecture guardrails,
  flag it even if not asked. Do not wait to be told.
- When proposing a fix, always state: is this a local patch or a systemic improvement?
  Label it clearly so the user can make an informed choice.

---

## Patch vs Refactor Decision

- Local patch = acceptable when: isolated bug, no architectural impact, low risk.
- Systemic refactor = required when: same problem appears in 3+ places,
  violates a guardrail, or will get worse as the codebase scales.
- Never propose only a local patch when a systemic fix is clearly the right answer.
  Propose both, explain the tradeoff, let the user decide.

---

## Execution Default

- When both a local patch and systemic refactor are presented,
  present the systemic option first, then the local patch.
- Do not start the broader refactor until the user explicitly confirms.
- If user intent is ambiguous, default to the safest reversible
  implementation and wait for confirmation before proceeding further.

---

## Tool Selection

- For multi-file mechanical changes (headers, imports, config blocks),
  prefer a script or programmatic approach over repeated exact-match edits.
- If an exact-match edit fails once, stop and re-read the exact current
  content before retrying — do not reduce snippet size blindly.
- If exact-match fails twice on the same location, switch approach entirely.
  Do not retry the same method more than twice.
- For refactors touching more than 2 files, always use a script
  approach from the start — never begin with exact-match edits.

---

## Refactor over 2 files = script only

- Any refactor touching more than 2 files must be implemented
  as a standalone Python/shell script first.
- Agent writes the script, you review it, you run it.
- No inline file edits for multi-file refactors. Ever.

---

## Implementation Discipline

- During any multi-step implementation, before each major decision point,
  state which rule is guiding the next step.
- If you cannot name the rule, stop and ask before proceeding.
- If an approach is taking more steps than expected, stop and reassess
  before continuing. Do not persist on a broken path.

---

## Uncertainty Handling

- If uncertain about current file state, read it first — do not assume.
- If uncertain about which approach is correct, present options and ask.
- Never proceed on an assumption without labeling it explicitly as an assumption.
- Uncertain + silent = forbidden. Uncertain + stated = acceptable.

---

## Anti-Compaction Safety Rule

For any task touching more than 1 file or more than 1 logical step:

### Before first edit

- Create or update `markdown/progress.md` with:
  - Task goal
  - Files in scope
  - Current on-disk status of each file
  - Planned changes per file
  - Risks and assumptions

### After every file edit

- Immediately update `markdown/progress.md` with:
  - File changed
  - Exact change made
  - Verification status
  - Next file in scope

### If session is getting long or compact may be near

- Stop editing
- Update `markdown/progress.md`
- Update `logs.md` if progress is meaningful
- Only then continue

### On resume or after any compaction

- Read `markdown/rule.md` → `markdown/project.md` → `markdown/logs.md` → `markdown/progress.md`
- Re-read exact active files on disk
- Do not rely on prior chat memory
- Never start a new edit until on-disk file state is confirmed
- Source of truth priority:
  1. On-disk file contents
  2. `markdown/progress.md`
  3. repo memory
  4. chat history last
- If `markdown/progress.md` conflicts with chat memory, trust `markdown/progress.md` until on-disk files are re-read.

### Phase structure for large refactors (3+ files)

- Split into explicit phases, checkpoint after each
- Do not start next phase until current phase is confirmed complete
- Example structure:
  - Phase 1 → read and audit only, no edits
  - Phase 2 → edit file 1-2, checkpoint
  - Phase 3 → edit file 3-4, checkpoint
  - Phase 4 → validate end-to-end
  - Phase 5 → deploy and verify
