import argparse
import json
import math
import os
import subprocess
import tempfile
from datetime import datetime, timezone

import pyarrow.parquet as pq

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


def histogram_counts(values, bin_edges):
    counts = [0 for _ in range(len(bin_edges) - 1)]
    for value in values:
        for i in range(len(bin_edges) - 1):
            left = bin_edges[i]
            right = bin_edges[i + 1]
            is_last = i == len(bin_edges) - 2
            if (left <= value < right) or (is_last and value == right):
                counts[i] += 1
                break
    return counts


def extract_values(rows):
    values = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_value = row.get("fare_amount")
        if raw_value is None:
            continue
        try:
            values.append(float(raw_value))
        except (TypeError, ValueError):
            continue
    return sorted(values)


def extract_values_from_parquet(path):
    table = pq.read_table(path, columns=["fare_amount"])
    return extract_values(table.to_pylist())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    summary_key = f"{args.state_prefix}/last_ingest_summary.json"
    generated_key = f"{args.state_prefix}/last_generated_summary.json"
    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_calc_")
    state_file = os.path.join(tmpdir, "current_rows.json")
    summary_file = os.path.join(tmpdir, "last_ingest_summary.json")
    row_parquet_file = os.path.join(tmpdir, "current_rows.parquet")
    calculation_file = os.path.join(tmpdir, "calculation_summary.json")
    generated_file = os.path.join(tmpdir, "last_generated_summary.json")

    aws_cp_from_s3(args.bucket, summary_key, summary_file)
    with open(summary_file, "r", encoding="utf-8") as f:
        ingest_summary = json.load(f)

    mode = str(ingest_summary.get("mode") or "")
    snapshot_label = str(ingest_summary.get("snapshot_label") or "")
    state_key = str(ingest_summary.get("state_key") or "")
    row_parquet_key = str(ingest_summary.get("row_state_key") or "")
    if mode not in {"file", "row"}:
        raise ValueError(f"Unsupported ingest mode: {mode or 'missing'}")
    if not snapshot_label:
        raise ValueError("Missing snapshot_label in last_ingest_summary")
    if not state_key:
        raise ValueError("Missing state_key in last_ingest_summary")
    if mode == "row" and not row_parquet_key:
        raise ValueError("Missing row_state_key in last_ingest_summary")

    aws_cp_from_s3(args.bucket, state_key, state_file)
    with open(state_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload if isinstance(payload, list) else []
    if mode == "row":
        aws_cp_from_s3(args.bucket, row_parquet_key, row_parquet_file)
        values = extract_values_from_parquet(row_parquet_file)
    else:
        values = extract_values(rows)

    min_edge = 0
    max_value = max(values) if values else 1.0
    max_edge = int(math.ceil(max(max_value, 10.0) / 2.0) * 2)
    if max_edge <= min_edge:
        max_edge = min_edge + 2
    bin_edges = list(range(min_edge, max_edge + 2, 2))
    if len(bin_edges) < 2:
        bin_edges = [0, 2]
    counts = histogram_counts(values, bin_edges)

    summary = {
        "feature": "fare_amount",
        "snapshot_id": snapshot_label,
        "mode": mode,
        "row_count": len(values),
        "values": values,
        "bin_edges": bin_edges,
        "counts": counts,
        "state_key": row_parquet_key if mode == "row" else state_key,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(calculation_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(generated_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "snapshot_id": snapshot_label,
                "mode": mode,
                "row_count": len(values),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "calculation_key": f"{args.snapshot_prefix}/{snapshot_label}/fare_amount/calculation/summary.json",
                "summary_key": f"{args.snapshot_prefix}/{snapshot_label}/fare_amount/summary.json",
            },
            f,
            indent=2,
        )

    base_key = f"{args.snapshot_prefix}/{snapshot_label}/fare_amount"
    aws_cp_to_s3(calculation_file, args.bucket, f"{base_key}/calculation/summary.json")
    if mode == "row":
        aws_cp_to_s3(row_parquet_file, args.bucket, f"{base_key}/source_rows.parquet")
    else:
        aws_cp_to_s3(state_file, args.bucket, f"{base_key}/source_rows.json")
    aws_cp_to_s3(generated_file, args.bucket, generated_key)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
