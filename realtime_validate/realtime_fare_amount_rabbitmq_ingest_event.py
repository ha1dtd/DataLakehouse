import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone

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


def save_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")


def load_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            rows.append(json.loads(stripped))
    return rows


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


def build_state_keys(state_prefix):
    return {
        "summary": f"{state_prefix}/last_ingest_summary.json",
        "file_state": f"{state_prefix}/file/current_file_event.json",
        "file_ids": f"{state_prefix}/file/processed_file_ids.json",
        "file_generated": f"{state_prefix}/file/last_generated_summary.json",
        "row_manifest": f"{state_prefix}/row/manifest.json",
        "row_generated": f"{state_prefix}/row/last_generated_summary.json",
    }


def load_optional_json(bucket, key, local_path, default_value):
    if not aws_exists(bucket, key):
        return default_value
    aws_cp_from_s3(bucket, key, local_path)
    return load_json(local_path)


def ensure_s3_source_path(metadata):
    source_path = str(metadata.get("source_path") or "")
    if not source_path.startswith("s3://"):
        raise ValueError(f"File mode requires MinIO/S3 pointer source, got: {source_path or 'missing'}")
    return source_path


def mode_outputs_ready(bucket, generated_key, generated_file):
    if not aws_exists(bucket, generated_key):
        return False
    aws_cp_from_s3(bucket, generated_key, generated_file)
    generated = load_json(generated_file)
    required_keys = [
        str(generated.get("calculation_key") or ""),
        str(generated.get("comparison_key") or ""),
        str(generated.get("summary_key") or ""),
        str(generated.get("chart_key") or ""),
    ]
    if any(not key for key in required_keys):
        return False
    return all(aws_exists(bucket, key) for key in required_keys)


def ingest_file_event(event_payload, metadata, processed_file_ids, outputs_ready):
    event_id = str(event_payload.get("event_id") or "")
    file_event_id = str(metadata.get("file_event_id") or event_id)
    duplicate = bool(file_event_id and file_event_id in processed_file_ids)
    source_path = ensure_s3_source_path(metadata)
    file_state = {
        "mode": "file",
        "event_id": event_id,
        "file_event_id": file_event_id,
        "source_path": source_path,
        "source_name": metadata.get("source_name"),
        "source_basename": metadata.get("source_basename"),
        "file_extension": metadata.get("file_extension"),
        "content_sha256": metadata.get("content_sha256"),
        "size_bytes": metadata.get("size_bytes"),
        "dataset": event_payload.get("dataset"),
        "ingested_at": utc_now_iso(),
    }
    if not duplicate and file_event_id:
        processed_file_ids.append(file_event_id)
    summary = {
        "mode": "file",
        "ingested_at": utc_now_iso(),
        "event_id": event_id,
        "event_key": None,
        "message_type": event_payload.get("message_type"),
        "source_name": metadata.get("source_name"),
        "file_event_id": file_event_id,
        "duplicate": duplicate,
        "applied_row_count": 0,
        "total_row_count_after": None,
        "should_calculate": (not duplicate) or (not outputs_ready),
        "snapshot_prefix": None,
        "event_payload_sha256": hashlib.sha256(json.dumps(event_payload, sort_keys=True).encode("utf-8")).hexdigest(),
    }
    return file_state, processed_file_ids, summary


def default_row_manifest():
    return {
        "state_format": "jsonl_chunks_v1",
        "chunk_keys": [],
        "event_id_chunk_keys": [],
        "row_count": 0,
        "replay_ids": [],
        "source_name": None,
        "source_path": None,
        "source_sha256": None,
        "updated_at": None,
    }


def load_processed_event_ids(bucket, manifest, tmpdir):
    processed_event_ids = set()
    for chunk_index, chunk_key in enumerate(manifest.get("event_id_chunk_keys") or [], start=1):
        local_file = os.path.join(tmpdir, f"event_ids_{chunk_index:05d}.jsonl")
        aws_cp_from_s3(bucket, chunk_key, local_file)
        for row in load_jsonl(local_file):
            event_id = str((row or {}).get("event_id") or "")
            if event_id:
                processed_event_ids.add(event_id)
    return processed_event_ids


def load_chunk_manifest(bucket, metadata, manifest_file):
    manifest_key = str(metadata.get("chunk_manifest_key") or "")
    if not manifest_key:
        raise ValueError("Row batch ingest requires metadata.chunk_manifest_key")
    aws_cp_from_s3(bucket, manifest_key, manifest_file)
    manifest = load_json(manifest_file)
    chunks = manifest.get("chunks") or []
    if not isinstance(chunks, list) or not chunks:
        raise ValueError("Row batch manifest is missing chunk definitions")
    return manifest, manifest_key


def write_state_chunk(bucket, key, rows, tmpdir, filename):
    local_file = os.path.join(tmpdir, filename)
    save_jsonl(local_file, rows)
    aws_cp_to_s3(local_file, bucket, key)
    return key


def apply_rows_to_state(bucket, state_prefix, replay_id, rows, row_manifest, processed_event_ids, tmpdir):
    applied_rows = []
    applied_event_id_rows = []
    for row in rows:
        normalized_event_id = str(row.get("event_id") or "")
        if normalized_event_id and normalized_event_id in processed_event_ids:
            continue
        if normalized_event_id:
            processed_event_ids.add(normalized_event_id)
            applied_event_id_rows.append({"event_id": normalized_event_id})
        applied_rows.append(row)
    if applied_rows:
        sequence = len(row_manifest["chunk_keys"]) + 1
        row_chunk_key = f"{state_prefix}/row/chunks/{replay_id}/chunk-{sequence:05d}.jsonl"
        event_id_chunk_key = f"{state_prefix}/row/event_ids/{replay_id}/chunk-{sequence:05d}.jsonl"
        write_state_chunk(bucket, row_chunk_key, applied_rows, tmpdir, f"applied_rows_{sequence:05d}.jsonl")
        write_state_chunk(bucket, event_id_chunk_key, applied_event_id_rows, tmpdir, f"applied_event_ids_{sequence:05d}.jsonl")
        row_manifest["chunk_keys"].append(row_chunk_key)
        row_manifest["event_id_chunk_keys"].append(event_id_chunk_key)
        row_manifest["row_count"] = int(row_manifest.get("row_count") or 0) + len(applied_rows)
    return len(applied_rows)


def ingest_row_message(bucket, state_prefix, event_payload, metadata, row_manifest, processed_event_ids, outputs_ready, tmpdir):
    replay_id = str(metadata.get("replay_id") or event_payload.get("event_id") or f"row-{int(datetime.now(timezone.utc).timestamp())}")
    message_type = str(event_payload.get("message_type") or "")
    total_applied_rows = 0
    source_name = str(metadata.get("source_name") or metadata.get("source_basename") or "source")
    if message_type == "row":
        normalized = normalize_row(
            event_payload.get("row") or {},
            str((event_payload.get("row") or {}).get("event_type") or "stream_ingest"),
            source_name,
            1,
        )
        total_applied_rows += apply_rows_to_state(
            bucket,
            state_prefix,
            replay_id,
            [normalized],
            row_manifest,
            processed_event_ids,
            tmpdir,
        )
    elif message_type == "row_batch":
        manifest_file = os.path.join(tmpdir, "row_batch_manifest.json")
        batch_manifest, manifest_key = load_chunk_manifest(bucket, metadata, manifest_file)
        metadata["chunk_manifest_key"] = manifest_key
        source_name = str(batch_manifest.get("source_name") or metadata.get("source_name") or "source")
        global_row_index = 0
        for chunk in batch_manifest.get("chunks") or []:
            chunk_key = str((chunk or {}).get("chunk_key") or "")
            if not chunk_key:
                continue
            local_file = os.path.join(tmpdir, f"source_chunk_{int(chunk.get('chunk_index') or 0):05d}.jsonl")
            aws_cp_from_s3(bucket, chunk_key, local_file)
            normalized_rows = []
            for raw_row in load_jsonl(local_file):
                global_row_index += 1
                normalized_rows.append(
                    normalize_row(
                        raw_row,
                        str(raw_row.get("event_type") or "stream_ingest"),
                        source_name,
                        global_row_index,
                    )
                )
            total_applied_rows += apply_rows_to_state(
                bucket,
                state_prefix,
                replay_id,
                normalized_rows,
                row_manifest,
                processed_event_ids,
                tmpdir,
            )
    else:
        raise ValueError(f"Unsupported row-mode message_type: {message_type}")

    duplicate = total_applied_rows == 0
    if replay_id and replay_id not in row_manifest["replay_ids"]:
        row_manifest["replay_ids"].append(replay_id)
    row_manifest["source_name"] = source_name
    row_manifest["source_path"] = metadata.get("source_path")
    row_manifest["source_sha256"] = metadata.get("source_sha256")
    row_manifest["updated_at"] = utc_now_iso()

    summary = {
        "mode": "row",
        "ingested_at": utc_now_iso(),
        "event_id": str(event_payload.get("event_id") or ""),
        "event_key": None,
        "message_type": event_payload.get("message_type"),
        "source_name": source_name,
        "file_event_id": None,
        "duplicate": duplicate,
        "applied_row_count": total_applied_rows,
        "total_row_count_after": int(row_manifest.get("row_count") or 0),
        "should_calculate": (total_applied_rows > 0) or (duplicate and not outputs_ready),
        "snapshot_prefix": None,
        "event_payload_sha256": hashlib.sha256(json.dumps(event_payload, sort_keys=True).encode("utf-8")).hexdigest(),
        "replay_id": replay_id,
        "chunk_manifest_key": metadata.get("chunk_manifest_key"),
    }
    return row_manifest, summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--event-key", required=True)
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    state_keys = build_state_keys(args.state_prefix)

    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_ingest_")
    event_file = os.path.join(tmpdir, "event.json")
    file_state_file = os.path.join(tmpdir, "current_file_event.json")
    file_ids_file = os.path.join(tmpdir, "processed_file_ids.json")
    file_generated_file = os.path.join(tmpdir, "file_last_generated_summary.json")
    row_manifest_file = os.path.join(tmpdir, "row_manifest.json")
    row_generated_file = os.path.join(tmpdir, "row_last_generated_summary.json")
    summary_file = os.path.join(tmpdir, "last_ingest_summary.json")

    aws_cp_from_s3(args.bucket, args.event_key, event_file)
    event_payload = load_json(event_file)
    message_type = str(event_payload.get("message_type") or "")
    metadata = event_payload.get("metadata") or {}
    if message_type == "file":
        processed_file_ids = load_optional_json(args.bucket, state_keys["file_ids"], file_ids_file, [])
        outputs_ready = mode_outputs_ready(args.bucket, state_keys["file_generated"], file_generated_file)
        file_state, processed_file_ids, summary = ingest_file_event(event_payload, metadata, processed_file_ids, outputs_ready)
        save_json(file_state_file, file_state)
        save_json(file_ids_file, processed_file_ids)
        aws_cp_to_s3(file_state_file, args.bucket, state_keys["file_state"])
        aws_cp_to_s3(file_ids_file, args.bucket, state_keys["file_ids"])
    elif message_type in {"row", "row_batch"}:
        row_manifest = load_optional_json(args.bucket, state_keys["row_manifest"], row_manifest_file, default_row_manifest())
        outputs_ready = mode_outputs_ready(args.bucket, state_keys["row_generated"], row_generated_file)
        processed_event_ids = load_processed_event_ids(args.bucket, row_manifest, tmpdir)
        row_manifest, summary = ingest_row_message(
            args.bucket,
            args.state_prefix,
            event_payload,
            metadata,
            row_manifest,
            processed_event_ids,
            outputs_ready,
            tmpdir,
        )
        save_json(row_manifest_file, row_manifest)
        aws_cp_to_s3(row_manifest_file, args.bucket, state_keys["row_manifest"])
    else:
        raise ValueError(f"Unsupported message_type: {message_type}")

    summary["event_key"] = args.event_key
    summary["snapshot_prefix"] = args.snapshot_prefix
    save_json(summary_file, summary)
    aws_cp_to_s3(summary_file, args.bucket, state_keys["summary"])

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
