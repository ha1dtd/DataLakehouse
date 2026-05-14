# FoxAI Agent Rules

## Startup Read Order

- Before any workday/session, read files in this exact order:
  1. `rule.md`
  2. `project.md`
  3. `logs.md`
- If asked to read `rule.md`, immediately continue by reading `project.md` next, then `logs.md` last.
- Remove/ignore any older startup instruction that conflicts with this order.

## Non-negotiables

- Make targeted edits only. No parallel copies (new/final/fixed).
- Read the exact active code before editing anything.
- Inspect the whole flow before patching multi-component behavior.
- Never claim fixed without a real verification path.
- After edits, check for editor errors.

## Deploy Rules

- Namenode access: `ssh nn` (assume shell is already on namenode when giving commands)
- DAG files → `/home/ubuntu/airflow/dags`
- Job scripts → `/home/ubuntu/daihai_script`
- Never push HTML files to namenode — served from local HTTP server only.
- After push, verify remote file content before confirming.

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

## Airflow Rules

- Airflow is orchestration only — DAGs trigger Spark jobs, nothing more.
- Forbidden: business logic or heavy transformations inside DAGs or PythonOperator.
- All tasks must be independently retryable.

## Config Rules

- No hardcoded credentials, endpoints, or environment-specific logic scattered in code.
- Use centralized config with environment separation (dev / staging / prod).

---

## Observability (required on every pipeline)

- Log: input row count, output row count, execution duration, failed record count, schema info.
- Validate: null rates, duplicate rates, schema consistency, partition validity.

---

## Code Quality

- Modular functions, reusable utilities, explicit naming, deterministic behavior.
- Forbidden: monolithic scripts, duplicated logic, magic constants, hidden side effects.

---

## AI Code Generation Rules

Before generating any code, always state:

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
