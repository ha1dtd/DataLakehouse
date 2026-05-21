# HDOS Source Findings

Last updated: 2026-05-20

## Purpose

This note records the HDOS PostgreSQL source findings already confirmed during the first `hdos_sample` investigation, so we do not need to rescan the database for the same baseline context.

Database connection used during verification:

- Host: `192.168.100.78`
- Port: `5630`
- Database: `test05052026`
- Schema used for hospital data review: `public`

## Confirmed Populated Tables

These tables were directly checked and confirmed to have data:

- `public.tb_patientrecord`
  - Confirmed row count: `763,887`
- `public.tb_servicedata`
  - Confirmed row count: `13,037,989`
- `public.tb_invoice`
  - Confirmed row count: `1,728,292`
- `public.tb_treatment`
  - Confirmed to have data
- `public.tb_nhanvien`
  - Confirmed to have data
- `public.tb_bed`
  - Confirmed to have data
- `public.tb_nhanvienlog`
  - Confirmed to have data
  - Used for the first connectivity/demo DAG

## Hospital-Meaningful Base Tables

These are the strongest confirmed hospital-related source tables for a business-ready medallion pipeline:

- `tb_patientrecord`
  - Patient encounter / patient record level data
  - Includes patient, encounter, diagnosis, admission/discharge, and billing summary fields
- `tb_servicedata`
  - Ordered/performed service activity
  - High-volume transactional fact table
- `tb_invoice`
  - Billing and payment facts
- `tb_treatment`
  - Treatment / clinical workflow data
  - Includes diagnosis, treatment dates, and treatment execution fields
- `tb_nhanvien`
  - Staff dimension / personnel lookup
- `tb_bed`
  - Bed / room / chamber / department resource data

## Current Interpretation

- `tb_nhanvienlog` is suitable as a technical connectivity demo.
- The more hospital-meaningful next version of `hdos_sample` should be built from:
  - `tb_patientrecord`
  - `tb_servicedata`
  - `tb_invoice`
  - `tb_treatment`
  - `tb_nhanvien`
- `tb_bed` is also meaningful and can be added depending on whether bed occupancy or room/department resource reporting is needed.

## Recommended Next Pipeline Shape

If we replace the current login demo with a hospital-facing sample, the recommended medallion flow is:

- Raw:
  - ingest each base table separately from PostgreSQL
- Bronze:
  - typed landing for each base table
- Silver:
  - join and clean patient, service, invoice, treatment, and staff data
- Gold:
  - expose hospital business outputs such as:
    - service revenue
    - patient counts
    - treatment counts
    - discharge/admission summaries

## Notes

- Earlier `pg_stat_user_tables` estimates were not reliable enough for source selection.
- Actual SQL reads and direct table checks were used as the source of truth for the findings above.
