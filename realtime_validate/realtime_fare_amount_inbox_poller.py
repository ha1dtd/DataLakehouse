import argparse
import json
import os
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "192.168.100.66:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "realtime_fare_amount_demo")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/opt/confluent/bin")
BATCH_EXTENSIONS = ("*.json", "*.parquet", "*.csv", "*.xml")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def sanitize_scalar(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def normalize_batch_row(row, source_file: Path, row_index: int):
    normalized = {str(k): sanitize_scalar(v) for k, v in row.items()}
    event_id = normalized.get("event_id") or normalized.get("trip_id") or f"{source_file.stem}-row-{row_index:04d}"
    trip_id = normalized.get("trip_id") or f"{source_file.stem}-trip-{row_index:04d}"
    normalized["event_id"] = str(event_id)
    normalized["trip_id"] = str(trip_id)
    normalized.setdefault("event_type", "batch_seed")
    normalized.setdefault("source_file_name", source_file.name)
    return normalized


def rows_from_json_payload(payload, source_file: Path):
    if isinstance(payload, dict) and "rows" in payload:
        rows = payload.get("rows", [])
    elif isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = [payload]
    else:
        raise ValueError(f"Unsupported JSON payload in {source_file}")
    return [normalize_batch_row(row, source_file, i) for i, row in enumerate(rows, start=1)]


def rows_from_parquet(path: Path):
    df = pd.read_parquet(path)
    return [normalize_batch_row(row, path, i) for i, row in enumerate(df.to_dict(orient="records"), start=1)]


def rows_from_csv(path: Path):
    df = pd.read_csv(path)
    return [normalize_batch_row(row, path, i) for i, row in enumerate(df.to_dict(orient="records"), start=1)]


def rows_from_xml(path: Path):
    tree = ET.parse(path)
    root = tree.getroot()
    rows = []
    for child in root:
        row = {grandchild.tag: grandchild.text for grandchild in child}
        if row:
            rows.append(row)
    if not rows:
        row = {child.tag: child.text for child in root}
        if row:
            rows.append(row)
    return [normalize_batch_row(row, path, i) for i, row in enumerate(rows, start=1)]


def load_batch_rows(path: Path):
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = load_json(path)
        return rows_from_json_payload(payload, path)
    if suffix == ".parquet":
        return rows_from_parquet(path)
    if suffix == ".csv":
        return rows_from_csv(path)
    if suffix == ".xml":
        return rows_from_xml(path)
    raise ValueError(f"Unsupported batch file extension: {path.suffix}")


def publish_message(message):
    producer_cmd = [
        f"{KAFKA_BIN}/kafka-console-producer",
        "--bootstrap-server",
        KAFKA_BOOTSTRAP,
        "--topic",
        KAFKA_TOPIC,
    ]
    payload = json.dumps(message) + "\n"
    subprocess.run(producer_cmd, input=payload.encode("utf-8"), check=True)


def normalize_batch_payload(rows, source_file):
    return {
        "message_type": "batch",
        "event_type": "batch_seed",
        "batch_id": source_file.stem,
        "source_file": str(source_file),
        "published_at": utc_now_iso(),
        "rows": rows,
    }


def normalize_row_payload(payload, source_file):
    row = dict(payload)
    row.setdefault("trip_id", row.get("event_id") or source_file.stem)
    row.setdefault("event_type", "stream_ingest")
    return {
        "message_type": "row",
        "event_type": row.get("event_type", "stream_ingest"),
        "event_id": row.get("event_id") or source_file.stem,
        "source_file": str(source_file),
        "published_at": utc_now_iso(),
        "row": row,
    }


def scan_and_publish(batch_dir: Path, row_dir: Path, processed_file: Path):
    processed = {"files": []}
    if processed_file.exists():
        processed = load_json(processed_file)
    done = set(processed.get("files", []))
    published = []

    batch_files = []
    for pattern in BATCH_EXTENSIONS:
        batch_files.extend(batch_dir.glob(pattern))

    for file_path in sorted({path.resolve() for path in batch_files}):
        file_key = str(file_path)
        if file_key in done:
            continue
        rows = load_batch_rows(file_path)
        message = normalize_batch_payload(rows, file_path)
        publish_message(message)
        done.add(file_key)
        published.append(file_key)

    for file_path in sorted(row_dir.glob("*.json")):
        file_key = str(file_path.resolve())
        if file_key in done:
            continue
        payload = load_json(file_path)
        message = normalize_row_payload(payload, file_path)
        publish_message(message)
        done.add(file_key)
        published.append(file_key)

    save_json(processed_file, {"files": sorted(done)})
    return published


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-dir", required=True)
    parser.add_argument("--row-dir", required=True)
    parser.add_argument("--processed-file", required=True)
    parser.add_argument("--interval-seconds", type=int, default=5)
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir)
    row_dir = Path(args.row_dir)
    processed_file = Path(args.processed_file)

    batch_dir.mkdir(parents=True, exist_ok=True)
    row_dir.mkdir(parents=True, exist_ok=True)
    processed_file.parent.mkdir(parents=True, exist_ok=True)

    while True:
        published = scan_and_publish(batch_dir, row_dir, processed_file)
        if published:
            print(json.dumps({"published_files": published, "published_count": len(published)}, indent=2), flush=True)
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
