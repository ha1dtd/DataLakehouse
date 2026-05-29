# Lakehouse Add-on Flow

## Purpose

This file describes the intended customer flow for using add-ons after the Lakehouse platform has been installed.

It explains:

- what the customer receives
- what the customer edits
- how the platform installs and enables add-ons
- how Airflow sees enabled add-ons

---

## 1. Platform Install First

The customer runs the Lakehouse installer first.

The installer sets up:

- the Lakehouse core platform
- Airflow
- Spark
- Hadoop / YARN
- MinIO / Iceberg base runtime
- the add-on runtime base folders

Required add-on runtime folders:

- `/opt/lakehouse/addons/`
- `/etc/lakehouse/addons/installed/`
- `/etc/lakehouse/addons/enabled/`
- `/etc/lakehouse/addons/config/`
- `/home/ubuntu/airflow/dags/addons/`

The installer prepares the runtime, but does not deploy customer add-ons.

---

## 2. Customer Receives an Add-on Template

After platform installation, the customer receives an add-on template package.

Example:

- `hdos_widget_addon`

This package is the customer-owned extension area.

The customer works inside the add-on package, not inside the platform core.

---

## 3. What the Customer Edits

The customer edits only add-on-owned files.

Main files and folders the customer works with:

- `manifest.yaml`
- `domains/.../dags/`
- `domains/.../jobs/`
- `domains/.../config/`
- `domains/.../sql/` if needed

The customer uses these areas for:

- DAG definitions
- Spark job logic
- SQL assets
- package defaults
- output definitions
- source-specific business logic

The customer must not edit:

- Airflow core config
- Spark core config
- Hadoop core config
- installer-managed files
- platform-owned runtime paths
- other add-ons

---

## 4. Customer Prepares the Add-on Package

The customer fills in the add-on package with:

- package identity in `manifest.yaml`
- domain DAG files
- job scripts
- default config
- SQL files if needed

The package is prepared outside the live platform runtime as a versioned add-on package.

This is a package-based deployment model, not a live script-editing workflow inside the platform.

---

## 5. Add-on Validation

Before the add-on becomes active, the platform should run `validate`.

Validation checks should include:

- required manifest fields exist
- required files and folders exist
- DAG ids are valid
- output paths and namespaces are declared
- dependencies are declared
- config files are complete enough to run
- package structure matches the add-on contract

If validation fails, the add-on must not be installed or enabled.

---

## 6. Add-on Install

After validation, the platform should run `install`.

Install behavior:

- copy the prepared add-on package into:
  - `/opt/lakehouse/addons/<addon-id>/<version>/`
- record installed metadata in:
  - `/etc/lakehouse/addons/installed/`

This step makes the package available to the platform, but not yet active in Airflow.

---

## 7. Add-on Enable

After install, the platform should run `enable`.

Enable behavior:

- mark one specific version as active in:
  - `/etc/lakehouse/addons/enabled/`
- expose only that add-on's DAG files into:
  - `/home/ubuntu/airflow/dags/addons/<addon-id>/`

Preferred v1 mechanism:

- symlink DAG files from the installed package into the managed Airflow add-on DAG path

Reason:

- simpler rollback
- simpler version switching
- less duplicated state than copying files

Only one version of a given add-on id may be enabled at a time.

---

## 8. How Airflow Sees the Add-on

Airflow should not scan random customer folders.

Airflow should see add-ons only through the managed enabled-DAG path:

- `/home/ubuntu/airflow/dags/addons/<addon-id>/`

After `enable`, Airflow discovers the DAGs through its normal DAG parsing cycle.

No special hot-reload mechanism is required for v1.

This means the platform picks up customer add-ons by:

- installing the package into the controlled add-on runtime path
- enabling one version
- exposing that version's DAG files into the managed Airflow add-on DAG folder

---

## 9. How Jobs and Config Resolve at Runtime

When an enabled DAG runs:

- the DAG should call job scripts from the installed package path under `/opt/lakehouse/addons/...`
- package default config should be read from the installed package
- environment-specific overrides should be read from:
  - `/etc/lakehouse/addons/config/<addon-id>/`
- real secrets should be injected from outside the package, not stored inside the add-on bundle

Runtime config resolution should be:

1. package defaults
2. environment overrides
3. external secret references

---

## 10. Disable, Remove, and Rollback

The platform should also support:

- `disable`
- `status`
- `remove`
- `rollback`

Expected behavior:

- `disable` removes the active DAG exposure from the Airflow add-on path
- `status` shows installed and enabled versions
- `remove` removes an installed version that is not active
- `rollback` switches activation back to a previous installed version

These operations must be auditable.

---

## 11. Summary Flow

The intended customer-facing flow is:

1. Customer runs the Lakehouse installer.
2. Installer prepares the platform and add-on runtime folders.
3. Customer receives an add-on template package.
4. Customer edits only add-on-owned files.
5. Platform runs `validate`.
6. Platform runs `install`.
7. Platform runs `enable`.
8. Enabled DAGs appear in the managed Airflow add-on DAG folder.
9. Airflow discovers the DAGs normally.
10. Jobs run from the installed package path.
11. Config overrides and secrets resolve from controlled external paths.

---

## 12. Current Status

This is the approved target flow.

Current repo status:

- the contract exists
- the first example add-on scaffold exists
- the actual add-on runtime commands are not implemented yet

Still to be implemented:

- `validate`
- `install`
- `enable`
- `disable`
- `status`
- `remove`
- `rollback`
- installer preparation for add-on runtime folders and permissions
