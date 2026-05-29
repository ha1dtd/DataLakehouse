# Lakehouse Add-on Templates

These templates are installer-provided examples for building customer add-on packages.

Do not edit these templates in place under `/opt/lakehouse/addons/templates/`.

Recommended flow:

1. Copy one template to your own working area.
2. Rename the package and update `manifest.yaml`.
3. Edit only add-on-owned files:
   - `manifest.yaml`
   - `domains/.../dags/`
   - `domains/.../jobs/`
   - `domains/.../config/`
   - `domains/.../sql/`
4. Keep platform-owned runtime paths and core configuration unchanged.
5. When the add-on runtime commands are available, use the platform flow to:
   - validate
   - install
   - enable

Included template:

- `starter_addon`
  - generic add-on template based on the real Lakehouse DAG/job/config setup pattern
  - mirrors the current multi-stage medallion flow shape used by `hdos_widget`
