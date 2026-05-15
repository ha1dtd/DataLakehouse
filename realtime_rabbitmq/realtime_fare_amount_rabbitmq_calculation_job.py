import argparse
import json
import math
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


def utc_now_compact():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    state_key = f"{args.state_prefix}/current_rows.json"
    generated_key = f"{args.state_prefix}/last_generated_summary.json"
    snapshot_id = utc_now_compact()
    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_calc_")
    state_file = os.path.join(tmpdir, "current_rows.json")
    calculation_file = os.path.join(tmpdir, "calculation_summary.json")
    generated_file = os.path.join(tmpdir, "last_generated_summary.json")

    aws_cp_from_s3(args.bucket, state_key, state_file)
    with open(state_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload if isinstance(payload, list) else []
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
    values = sorted(values)

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
        "snapshot_id": snapshot_id,
        "row_count": len(values),
        "values": values,
        "bin_edges": bin_edges,
        "counts": counts,
        "state_key": state_key,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(calculation_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(generated_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "snapshot_id": snapshot_id,
                "row_count": len(values),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "calculation_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/calculation/summary.json",
                "summary_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/summary.json",
            },
            f,
            indent=2,
        )

    base_key = f"{args.snapshot_prefix}/{snapshot_id}/fare_amount"
    aws_cp_to_s3(calculation_file, args.bucket, f"{base_key}/calculation/summary.json")
    aws_cp_to_s3(state_file, args.bucket, f"{base_key}/source_rows.json")
    aws_cp_to_s3(generated_file, args.bucket, generated_key)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
