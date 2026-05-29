# HDOS Widget API

This folder contains a thin read-only API adapter for the `hdos_widget` dashboard.

## Purpose

- Airflow writes the latest dashboard snapshot to MinIO
- this API reads that snapshot from MinIO
- the frontend calls `GET /api/screen/dashboard`

The API does not query Spark, PostgreSQL, or Iceberg directly. It only serves the latest exported JSON snapshot.

## Current endpoint

- `GET /health`
- `GET /api/screen/{screen_id}`
  - first expected screen id: `dashboard`

## Data source

The API reads the snapshot written by `dags/hdos_widget/gold_to_json.py`.

Default object location:

- bucket: `gold`
- keys:
  - `lakehouse/serving/hdos_widget/screen/dashboard.json`
  - `lakehouse/serving/hdos_widget/screen/dashboard_fe.json`

That default comes from `../hdos_widget_config.json`:

- `JSON_EXPORT_BASE=s3a://gold/lakehouse/serving/hdos_widget/`

Current route mapping:

- `GET /api/screen/dashboard`
  - serves `dashboard_fe.json`
- `GET /api/screen/dashboard_fe`
  - serves `dashboard_fe.json`
- `GET /api/screen/dashboard_raw`
  - can be used later if a separate raw/hydrated export is added under that object name

## Install

Create a Python environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r dags/hdos_widget/api/requirements.txt
```

## Configuration

The API loads defaults from:

- `dags/hdos_widget/hdos_widget_config.json`

You can override them with environment variables:

```bash
export MINIO_ENDPOINT='http://192.168.100.66:9001'
export MINIO_ACCESS_KEY='admin'
export MINIO_SECRET_KEY='12345678'
export HDOS_SCREEN_BUCKET='gold'
export HDOS_SCREEN_PREFIX='lakehouse/serving/hdos_widget'
```

Optional:

```bash
export HDOS_API_CONFIG_FILE='/path/to/hdos_widget_config.json'
export AWS_REGION='us-east-1'
```

## Run

From the repo root:

```bash
source .venv/bin/activate
uvicorn dags.hdos_widget.api.app:app --host 0.0.0.0 --port 8000
```

If you run from inside `dags/hdos_widget/api/`, this also works:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

## Test

Health check:

```bash
curl http://127.0.0.1:8000/health
```

Dashboard payload:

```bash
curl http://127.0.0.1:8000/api/screen/dashboard
```

## Deployment note

If the frontend already expects `/api/screen/dashboard` on the same origin, place this service behind the frontend reverse proxy so the path stays unchanged.

If you expose it on a different host or port, the frontend must call that base URL instead, and you may need CORS configuration based on the actual browser request flow.
