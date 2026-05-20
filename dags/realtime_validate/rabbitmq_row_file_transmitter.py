import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pika
import pyarrow.parquet as pq

DEFAULT_HOST = os.environ.get("FOXAI_RABBITMQ_HOST", "192.168.100.60")
DEFAULT_PORT = int(os.environ.get("FOXAI_RABBITMQ_PORT", "5672"))
DEFAULT_VHOST = os.environ.get("FOXAI_RABBITMQ_VHOST", "/")
DEFAULT_USER = os.environ.get("FOXAI_RABBITMQ_USER", "guest")
DEFAULT_PASS = os.environ.get("FOXAI_RABBITMQ_PASS", "guest")
DEFAULT_QUEUE = os.environ.get("FOXAI_RABBITMQ_QUEUE", "daihai_local_test_1")
DEFAULT_DATASET = os.environ.get("FOXAI_RABBITMQ_DATASET", "fare_amount_realtime_demo")
DEFAULT_BUCKET = os.environ.get("FOXAI_MINIO_BUCKET", "histogram")
MINIO_ENDPOINT = os.environ.get("FOXAI_MINIO_ENDPOINT", "http://192.168.100.66:9001")
MINIO_ACCESS_KEY = os.environ.get("FOXAI_MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("FOXAI_MINIO_SECRET_KEY", "12345678")
TEMP_S3_PREFIX = "foxai_s3_"
ROW_FILE_CHUNK_PREFIX = os.environ.get("FOXAI_ROW_FILE_CHUNK_PREFIX", "demo/realtime_validate_fare_amount/replay_chunks")
DEFAULT_ROW_CHUNK_SIZE = int(os.environ.get("FOXAI_ROW_FILE_CHUNK_SIZE", "5000"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(DEFAULT_USER, DEFAULT_PASS)
    params = pika.ConnectionParameters(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        virtual_host=DEFAULT_VHOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def sanitize_scalar(value):
    if value is None:
        return None
    if hasattr(value, "as_py"):
        try:
            value = value.as_py()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return value


def stable_row_event_id(row: dict, row_index: int, source_name: str) -> str:
    if row.get("event_id"):
        return str(row["event_id"])
    base = json.dumps(row, sort_keys=True, ensure_ascii=False, default=str)
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
    return f"row-{Path(source_name).stem}-{row_index:08d}-{digest}"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cleanup_temp_path(path_str: str) -> None:
    if not path_str:
        return
    path = Path(path_str)
    if TEMP_S3_PREFIX not in path.name:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def aws_cp_to_s3(local_path: str, bucket: str, key: str) -> None:
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


def materialize_parquet_source(source: str) -> tuple[Path, str, str]:
    parsed = urlparse(source)
    if parsed.scheme == "s3":
        suffix = Path(parsed.path).suffix or ".parquet"
        with tempfile.NamedTemporaryFile(prefix=TEMP_S3_PREFIX, suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
        cmd = [
            "bash",
            "-lc",
            (
                f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
                f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
                f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp '{source}' '{tmp_path}'"
            ),
        ]
        subprocess.run(cmd, check=True)
        source_name = os.path.basename(parsed.path) or Path(tmp_path).name
        return Path(tmp_path), source, source_name
    path = Path(source).expanduser().resolve()
    return path, str(path), path.name


def stage_row_chunks(source: str, row_chunk_size: int) -> tuple[list[dict], str, str, str, str, int]:
    path, resolved_source, source_name = materialize_parquet_source(source)
    try:
        parquet = pq.ParquetFile(path)
        source_sha256 = file_sha256(path)
        replay_id = f"rowfile-{source_sha256[:16]}"
        chunk_prefix = f"{ROW_FILE_CHUNK_PREFIX}/{replay_id}"
        row_index = 0
        uploaded_chunks = []
        total_row_count = 0
        for batch in parquet.iter_batches(batch_size=row_chunk_size):
            rows = []
            for row in batch.to_pylist():
                row_index += 1
                normalized = {str(k): sanitize_scalar(v) for k, v in dict(row).items()}
                normalized.setdefault("source_file_name", source_name)
                normalized.setdefault("source_row_index", row_index)
                normalized["event_id"] = stable_row_event_id(normalized, row_index, source_name)
                rows.append(normalized)
            if rows:
                total_row_count += len(rows)
                chunk_index = len(uploaded_chunks) + 1
                with tempfile.NamedTemporaryFile(prefix="row_chunk_", suffix=".jsonl", delete=False) as tmp:
                    tmp_path = tmp.name
                try:
                    with open(tmp_path, "w", encoding="utf-8") as handle:
                        for normalized in rows:
                            handle.write(json.dumps(normalized, ensure_ascii=False))
                            handle.write("\n")
                    chunk_key = f"{chunk_prefix}/chunk-{chunk_index:05d}.jsonl"
                    aws_cp_to_s3(tmp_path, DEFAULT_BUCKET, chunk_key)
                    uploaded_chunks.append(
                        {
                            "chunk_index": chunk_index,
                            "chunk_key": chunk_key,
                            "row_count": len(rows),
                        }
                    )
                finally:
                    cleanup_temp_path(tmp_path)
        return uploaded_chunks, resolved_source, source_name, source_sha256, replay_id, total_row_count
    finally:
        cleanup_temp_path(str(path))


def upload_chunk_manifest(
    *,
    replay_id: str,
    resolved_source: str,
    source_name: str,
    source_sha256: str,
    uploaded_chunks: list[dict],
) -> str:
    manifest_key = f"{ROW_FILE_CHUNK_PREFIX}/{replay_id}/manifest.json"
    payload = {
        "replay_id": replay_id,
        "source_path": resolved_source,
        "source_name": source_name,
        "source_basename": Path(source_name).stem,
        "file_extension": Path(source_name).suffix.lower(),
        "source_sha256": source_sha256,
        "chunk_count": len(uploaded_chunks),
        "total_row_count": sum(int(chunk.get("row_count") or 0) for chunk in uploaded_chunks),
        "chunks": uploaded_chunks,
        "created_at": utc_now(),
    }
    with tempfile.NamedTemporaryFile(prefix="row_manifest_", suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        aws_cp_to_s3(tmp_path, DEFAULT_BUCKET, manifest_key)
    finally:
        cleanup_temp_path(tmp_path)
    return manifest_key


def send_message(channel, payload: dict) -> dict:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    message_id = hashlib.sha256(body).hexdigest()[:24]
    channel.basic_publish(
        exchange="",
        routing_key=DEFAULT_QUEUE,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=message_id,
            timestamp=int(datetime.now(timezone.utc).timestamp()),
        ),
    )
    result = {
        "status": "sent",
        "queue": DEFAULT_QUEUE,
        "message_id": message_id,
        "message_type": payload.get("message_type"),
        "event_id": payload.get("event_id"),
        "metadata": payload.get("metadata"),
    }
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


def send_row_file(source: str) -> int:
    uploaded_chunks, resolved_path, source_name, source_sha256, replay_id, total_row_count = stage_row_chunks(source, DEFAULT_ROW_CHUNK_SIZE)
    manifest_key = upload_chunk_manifest(
        replay_id=replay_id,
        resolved_source=resolved_path,
        source_name=source_name,
        source_sha256=source_sha256,
        uploaded_chunks=uploaded_chunks,
    )

    conn = build_connection()
    channel = conn.channel()
    channel.queue_declare(queue=DEFAULT_QUEUE, durable=True)
    try:
        payload = {
            "message_type": "row_batch",
            "event_id": replay_id,
            "event_ts": utc_now(),
            "dataset": DEFAULT_DATASET,
            "source_type": "s3" if urlparse(source).scheme == "s3" else "local_path",
            "metadata": {
                "replay_id": replay_id,
                "source_path": resolved_path,
                "source_name": source_name,
                "source_basename": Path(source_name).stem,
                "file_extension": Path(source_name).suffix.lower(),
                "source_sha256": source_sha256,
                "chunk_prefix": f"s3://{DEFAULT_BUCKET}/{ROW_FILE_CHUNK_PREFIX}/{replay_id}",
                "chunk_manifest_key": manifest_key,
                "chunk_count": len(uploaded_chunks),
                "total_row_count": total_row_count,
            },
        }
        send_message(channel, payload)
    finally:
        conn.close()

    print(
        json.dumps(
            {
                "status": "completed",
                "queue": DEFAULT_QUEUE,
                "source_path": resolved_path,
                "replay_id": replay_id,
                "chunk_count": len(uploaded_chunks),
                "rows_sent": total_row_count,
                "manifest_key": manifest_key,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return total_row_count


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--row-file", required=True, help="Path to single parquet file to replay as row messages")
    args = parser.parse_args(argv)
    send_row_file(args.row_file)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
