import argparse
import base64
import hashlib
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlparse

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"


def aws_cp_from_s3(bucket, key, local_path):
    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp s3://{bucket}/{key} '{local_path}'"
        ),
    ]
    subprocess.run(cmd, check=True)


def aws_cp_to_s3(local_path, bucket, key):
    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp '{local_path}' s3://{bucket}/{key}"
        ),
    ]
    subprocess.run(cmd, check=True)


def aws_exists(bucket, key):
    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 ls s3://{bucket}/{key} >/dev/null 2>&1"
        ),
    ]
    result = subprocess.run(cmd)
    return result.returncode == 0


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def sanitize_scalar(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def normalize_row(row, fallback_event_type, source_name, row_index):
    normalized = {str(k): sanitize_scalar(v) for k, v in dict(row).items()}
    event_id = normalized.get("event_id") or normalized.get("trip_id") or f"{source_name}-row-{row_index:04d}"
    trip_id = normalized.get("trip_id") or f"{source_name}-trip-{row_index:04d}"
    normalized["event_id"] = str(event_id)
    normalized["trip_id"] = str(trip_id)
    normalized.setdefault("event_type", fallback_event_type)
    return normalized


def rows_from_json_text(text, source_name):
    payload = json.loads(text)
    if isinstance(payload, dict) and "rows" in payload:
        rows = payload.get("rows", [])
        event_type = str(payload.get("event_type") or "batch_seed")
    elif isinstance(payload, list):
        rows = payload
        event_type = "batch_seed"
    elif isinstance(payload, dict):
        rows = [payload]
        event_type = str(payload.get("event_type") or "stream_ingest")
    else:
        raise ValueError("Unsupported JSON file payload")
    return [normalize_row(row, event_type, source_name, i) for i, row in enumerate(rows, start=1)]


def extract_rows(event_payload):
    message_type = str(event_payload.get("message_type") or "")
    metadata = event_payload.get("metadata") or {}
    source_name = str(metadata.get("source_name") or metadata.get("source_basename") or "source")

    if message_type == "row":
        row = event_payload.get("row") or {}
        normalized = normalize_row(row, str(row.get("event_type") or "stream_ingest"), source_name, 1)
        return [normalized], "row"

    if message_type != "file":
        raise ValueError(f"Unsupported message_type: {message_type}")

    encoding = str(event_payload.get("content_encoding") or "utf-8")
    raw_b64 = str(event_payload.get("content_base64") or "")
    if not raw_b64:
        raise ValueError("Missing file content_base64")
    raw_bytes = base64.b64decode(raw_b64)
    suffix = str(metadata.get("file_extension") or "").lower()

    if suffix != ".json":
        raise ValueError(f"Unsupported file extension for RabbitMQ file ingest: {suffix}")

    text = raw_bytes.decode(encoding or "utf-8")
    return rows_from_json_text(text, source_name), "file"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--event-key", required=True)
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    state_key = f"{args.state_prefix}/current_rows.json"
    event_ids_key = f"{args.state_prefix}/processed_event_ids.json"
    file_ids_key = f"{args.state_prefix}/processed_file_ids.json"
    summary_key = f"{args.state_prefix}/last_ingest_summary.json"

    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_ingest_")
    event_file = os.path.join(tmpdir, "event.json")
    state_file = os.path.join(tmpdir, "current_rows.json")
    event_ids_file = os.path.join(tmpdir, "processed_event_ids.json")
    file_ids_file = os.path.join(tmpdir, "processed_file_ids.json")
    summary_file = os.path.join(tmpdir, "last_ingest_summary.json")

    aws_cp_from_s3(args.bucket, args.event_key, event_file)
    event_payload = load_json(event_file)

    rows = []
    processed_event_ids = []
    processed_file_ids = []

    if aws_exists(args.bucket, state_key):
        aws_cp_from_s3(args.bucket, state_key, state_file)
        rows = load_json(state_file)
    if aws_exists(args.bucket, event_ids_key):
        aws_cp_from_s3(args.bucket, event_ids_key, event_ids_file)
        processed_event_ids = load_json(event_ids_file)
    if aws_exists(args.bucket, file_ids_key):
        aws_cp_from_s3(args.bucket, file_ids_key, file_ids_file)
        processed_file_ids = load_json(file_ids_file)

    incoming_rows, ingest_kind = extract_rows(event_payload)
    metadata = event_payload.get("metadata") or {}
    event_id = str(event_payload.get("event_id") or "")
    file_event_id = str(metadata.get("file_event_id") or event_id)
    applied_rows = 0
    duplicate = False

    if ingest_kind == "file":
        if file_event_id and file_event_id in processed_file_ids:
            duplicate = True
        else:
            rows = []
            processed_event_ids = []
            for row in incoming_rows:
                row_event_id = str(row.get("event_id") or "")
                if row_event_id:
                    processed_event_ids.append(row_event_id)
                rows.append(row)
                applied_rows += 1
            if file_event_id:
                processed_file_ids.append(file_event_id)
    else:
        row = incoming_rows[0]
        row_event_id = str(row.get("event_id") or event_id)
        if row_event_id and row_event_id in processed_event_ids:
            duplicate = True
        else:
            rows.append(row)
            if row_event_id:
                processed_event_ids.append(row_event_id)
            applied_rows = 1

    summary = {
        "ingested_at": utc_now_iso(),
        "event_key": args.event_key,
        "event_id": event_id,
        "message_type": event_payload.get("message_type"),
        "applied_row_count": applied_rows,
        "total_row_count_after": len(rows),
        "duplicate": duplicate,
        "should_calculate": applied_rows > 0,
        "snapshot_prefix": args.snapshot_prefix,
        "source_name": metadata.get("source_name"),
        "file_event_id": file_event_id if ingest_kind == "file" else None,
        "event_payload_sha256": hashlib.sha256(json.dumps(event_payload, sort_keys=True).encode("utf-8")).hexdigest(),
    }

    save_json(state_file, rows)
    save_json(event_ids_file, processed_event_ids)
    save_json(file_ids_file, processed_file_ids)
    save_json(summary_file, summary)

    aws_cp_to_s3(state_file, args.bucket, state_key)
    aws_cp_to_s3(event_ids_file, args.bucket, event_ids_key)
    aws_cp_to_s3(file_ids_file, args.bucket, file_ids_key)
    aws_cp_to_s3(summary_file, args.bucket, summary_key)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
