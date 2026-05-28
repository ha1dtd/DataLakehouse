# Lakehouse Add-on Contract

## Purpose

This file records the current agreement for the Lakehouse add-on feature.

An add-on is a customer-owned package that runs on top of the installed Lakehouse platform without modifying the Lakehouse core installed by the installer.

This contract is the source of truth for:

- what an add-on is
- what the Lakehouse core owns
- what an add-on owns
- how add-ons are packaged, enabled, and isolated
- what the v1 production boundary is

---

## 1. Agreed Direction

### 1.1 Add-on unit

Current agreed direction:

- the top-level add-on unit is one customer package
- one customer package may contain multiple domains
- each domain may contain multiple DAGs, jobs, configs, SQL files, and related logic

Implication:

- Lakehouse should not model the add-on as "one DAG only"
- the add-on system must support nested structure inside one customer package

### 1.2 Supported scope

Current agreed direction:

- the add-on architecture should be broad enough to support:
  - DAGs
  - Spark jobs
  - SQL bundles
  - custom services

Implementation note:

- the contract allows all of the above at the architecture level
- the v1 implementation scope is narrower:
  - DAGs
  - Spark jobs
  - SQL bundles
  - config
- custom services are reserved for a later phase
- the package structure and manifest must still leave room for custom services later

### 1.3 Ownership boundary

Lakehouse core owns everything installed and managed by the installer, including:

- Airflow runtime
- Spark runtime
- Hadoop/YARN runtime
- MinIO/Iceberg base configuration
- shared environment variables
- shared paths
- shared dependency packages
- installer-managed configuration and machine state

Add-on owns:

- DAG files
- Spark job scripts
- SQL files
- source-specific config
- business logic
- output table definitions
- scheduling choices
- domain-specific pipeline behavior

Hard rule:

- an add-on cannot modify files or settings owned by the Lakehouse installer
- an add-on cannot modify other add-ons

---

## 2. Package Shape

### 2.1 Required structure

Current contract for the add-on package shape:

- use an industry-standard structured bundle
- add-ons are versioned deployable packages, not ad hoc script collections
- the proposed structure is accepted as the baseline because it is scalable and operationally clear

Required baseline structure:

- `manifest.yaml`
- `README.md`
- `domains/`

Inside each domain:

- `dags/`
- `jobs/`
- `config/`
- `sql/` optional
- `services/` optional

Example shape:

```text
<addon-id>/
  manifest.yaml
  README.md
  domains/
    <domain-a>/
      dags/
      jobs/
      config/
      sql/
      services/
    <domain-b>/
      dags/
      jobs/
      config/
```

### 2.2 Deployment model

Current agreed direction:

- the product model is package-based deployment
- customer teams author and prepare the add-on outside the platform runtime
- Lakehouse validates, installs, enables, disables, and removes add-on packages

Current contract for v1:

- v1 does not use an installer-style interactive script-entry workflow
- v1 does not treat the platform as a code authoring panel
- v1 accepts a prepared add-on package and deploys it into controlled runtime paths
- a scaffold/generator tool may be added later, but it is not part of the core runtime contract

### 2.3 Deployment path

Current contract:

- deployed add-ons must live in controlled product-owned paths
- desktop paths or ad hoc user-created runtime locations are not acceptable for the product contract

Required v1 paths:

- managed add-on storage:
  - `/opt/lakehouse/addons/<addon-id>/<version>/`
- central enabled-addons registry:
  - `/etc/lakehouse/addons/enabled/`
- central installed-addons registry or metadata path:
  - `/etc/lakehouse/addons/installed/`
- Airflow-exposed DAG path:
  - `/home/ubuntu/airflow/dags/addons/<addon-id>/`

Reason:

- predictable permissions and ownership
- stable backup and audit behavior
- clear separation between platform-owned runtime paths and customer-authored package content

### 2.4 Airflow exposure

Current agreed direction:

- enabled DAGs should be exposed into a dedicated Airflow add-on area

Current implementation preference:

- use a managed Airflow add-on folder:
  - `/home/ubuntu/airflow/dags/addons/<addon-id>/`
- populate that path through enable/disable operations

---

## 3. Manifest Contract

### 3.1 Manifest requirement

Current contract:

- every add-on must have a mandatory `manifest.yaml`

Reason:

- this is the most practical industry-standard direction for packaged extensions
- it gives a machine-readable contract for validation, enable/disable behavior, compatibility checks, dependency declaration, and later licensing hooks

### 3.2 Required manifest fields for v1

Current required fields:

- `id`
- `name`
- `version`
- `type`
- `owner`
- `domains`
- `entry_dags`
- `jobs_path`
- `config_files`
- `required_env`
- `required_connections`
- `required_packages`
- `target_namespaces`
- `output_locations`
- `min_platform_version`
- `license_hooks` optional
- `shared_output_policy`

Field notes:

- `domains`: declares the domain list inside the add-on package
- `required_packages`: declares extra Python/OS packages if needed
- `output_locations`: declares where outputs are intended to land
- `min_platform_version`: declares compatibility with the Lakehouse platform version

### 3.3 Output declaration

Current contract:

- the manifest must declare output locations so the user knows where the add-on writes data

At minimum this should cover:

- MinIO prefixes
- Iceberg catalogs / namespaces / databases
- local working directories if used

---

## 4. Runtime Activation Model

### 4.1 Activation

Current contract:

- add-ons become active through both:
  - an enable command/tool
  - a central enabled-addons registry/config

That means the architecture should support:

- validation
- install
- enable
- disable
- list
- remove

### 4.2 Minimum v1 operations

Current minimum supported operations for v1:

- `validate`
- `install`
- `enable`
- `disable`
- `list`
- `remove`
- `status`
- `rollback`

### 4.3 Reload behavior

Current interpretation:

- v1 does not need true hot reload
- v1 only needs normal Airflow discovery behavior after enable/disable

Practical meaning:

- after enabling an add-on, Airflow should detect the DAGs through its normal DAG parsing cycle
- no special in-memory runtime patching is required

---

## 5. Isolation and Safety

### 5.1 Failure boundary

Current contract:

- if one add-on fails, only that add-on's DAG/task should fail
- the failure should be logged clearly so the operator can see why it failed

Not allowed:

- one add-on failure must not stop all other add-ons automatically

### 5.2 Namespace boundaries

Current contract:

- each add-on should have clear namespace boundaries by default

Recommended boundaries:

- DAG id prefix
- MinIO prefix
- Iceberg namespace/database
- local temp/work directory
- log prefix

Interpretation:

- this question is about preventing collisions between add-ons
- by default, add-ons should not write into the same DAG ids, the same storage prefixes, or the same working paths unless the user intentionally configures that

### 5.3 Shared outputs

Current contract:

- two add-ons may share the same tables or storage prefixes only through an explicit override
- shared outputs must not be the default behavior

Safety note:

- default design should still prefer isolated targets
- shared targets should be treated as an explicit override, not the default
- shared targets should require:
  - explicit declaration in the manifest
  - operator confirmation during validation/install/enable flow

### 5.4 Extra dependencies

Current contract:

- dependencies must be declared first
- undeclared dependency installation is not allowed
- the platform must surface what is required before installation or enablement
- the user must be able to approve or reject eligible dependency installation

Implementation direction:

- declared dependencies should be read from the manifest
- the platform should validate machine state and print what is missing before installation/enablement
- Python-level dependencies may be supported through a controlled mechanism in v1
- OS-level dependencies must be treated more strictly:
  - they are not freely installable by add-ons
  - they must go through a platform-approved admin workflow
  - the add-on cannot mutate core runtime packages on its own

---

## 6. Configuration and Secrets

### 6.1 Config location

Current contract:

- the add-on bundle may include default config
- environment-specific override config must live outside the versioned add-on bundle

Required v1 direction:

- bundle defaults live under the deployed add-on version path
- environment override files live in a controlled external config path
- runtime should resolve configuration as:
  - bundle defaults
  - then environment overrides

### 6.2 Environment-overridable values

Current contract:

- the following values must be environment-overridable:
- source database host
- source database port
- source database username
- schedules
- output namespaces
- resource sizing
- feature flags

Reason:

- these are the values most likely to differ across customers or environments without changing the add-on logic itself

### 6.3 Secrets

Current contract:

- secrets must not be stored inside the packaged add-on bundle

Meaning:

- passwords, tokens, private keys, and other secrets should live outside normal versioned add-on files when possible

Reason:

- easier rotation
- safer separation from deployable code
- better fit for future licensing/security controls

Implementation note:

- secret placeholders or references may exist in bundle config
- real secret values must be injected from outside the bundle

---

## 7. Versioning and Compatibility

### 7.1 Multiple versions on disk

Current contract:

- multiple versions of the same add-on may exist on disk at the same time

### 7.2 Enabled version rule

Current contract:

- only one version may be enabled at a time for a given add-on id

### 7.3 Platform compatibility

Current contract:

- the manifest must declare compatibility with a Lakehouse platform version

---

## 8. Licensing Boundary

### 8.1 Licensing phase

Current contract:

- licensing is not part of the first add-on implementation
- the add-on implementation must still leave space for licensing later

### 8.2 Future licensing gate

Current direction:

- licensing will gate the whole project, not just add-ons
- nothing should start or be usable until the license is valid

Implication for add-on architecture:

- add-on install/enable/runtime flows should be designed so global license checks can be inserted later

---

## 9. First Implementation Scope

### 9.1 First example add-on

Current agreed example:

- `hdos_widget`

### 9.2 How to derive the first example

Current contract:

- use the current repo as source inspiration
- do not directly replicate the current DAG package structure as-is
- reshape it into the add-on structure because the current DAG folders were not designed as add-ons yet

### 9.3 Baseline success condition for v1

Current agreed baseline:

- platform already installed
- add-on bundle placed on server
- add-on validated
- add-on enabled
- Airflow sees the DAG
- job runs successfully
- outputs land in isolated target paths by default

Additional product-grade success conditions:

- all package metadata is auditable
- enable/disable/install/remove actions are recorded
- only one version is enabled for a given add-on id
- rollback to a previous installed version is possible
- no add-on action mutates installer-owned core files

---

## 10. Implementation Notes For Lakehouse v1

These are contract-aligned implementation notes, not open direction questions:

- add-ons are deployed as prepared versioned packages
- v1 runtime scope is:
  - DAGs
  - Spark jobs
  - SQL
  - config
- custom services are allowed by architecture but deferred from the first implementation pass
- add-on deployment and activation must be auditable
- dependency handling must be controlled and explicit
- environment-specific configuration and secrets must stay outside the packaged version payload

---

## 11. Current v1 Recommendation

Based on the approved direction, the v1 implementation path is:

- top-level unit: one customer add-on package with multiple domains
- prepared versioned package deployment model
- mandatory `manifest.yaml`
- structured bundle under `domains/`
- controlled runtime paths under `/opt/lakehouse` and `/etc/lakehouse`
- enable/disable model with central registry
- dedicated Airflow add-on DAG path
- default isolation by DAG prefix + storage prefix + namespace
- declared dependencies only
- controlled OS dependency policy
- externalized environment overrides and secrets
- auditability for install/enable/disable/remove/rollback
- no core-file mutation by add-ons
- first example based on `hdos_widget`
