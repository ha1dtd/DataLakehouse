import argparse
import json
import math
import os
import subprocess
import tempfile
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"


def build_spark():
    return SparkSession.builder \
        .appName("RealtimeFareAmountHistogramDemo") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


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


def format_thousands_dot(v, _pos=None):
    try:
        return f"{int(round(v)):,}".replace(",", ".")
    except Exception:
        return str(v)


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


def render_histogram(values, title, subtitle, out_file, bin_edges):
    counts = histogram_counts(values, bin_edges)
    centers = [(bin_edges[i] + bin_edges[i + 1]) / 2.0 for i in range(len(bin_edges) - 1)]
    width = (bin_edges[1] - bin_edges[0]) * 0.92

    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(1, 1, figsize=(14.0, 9.0), facecolor="#f6f9ff")
    ax.set_facecolor("#eef4ff")
    ax.bar(centers, counts, width=width, color="#3b82f6", edgecolor="#1e3a8a", linewidth=0.6)
    ax.set_title(title, fontsize=22, fontweight="bold", color="#0f172a")
    ax.set_xlabel("Fare Amount (USD $)", fontsize=16)
    ax.set_ylabel("Number of records", fontsize=16)
    ax.tick_params(axis="both", labelsize=13)
    ax.yaxis.set_major_formatter(FuncFormatter(format_thousands_dot))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=10, integer=True, min_n_ticks=5))
    ax.set_xticks(bin_edges)
    ax.set_xlim(left=bin_edges[0], right=bin_edges[-1])
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    fig.text(0.5, 0.02, subtitle, ha="center", va="bottom", fontsize=11, color="#0f172a")
    plt.tight_layout(rect=[0.01, 0.04, 0.995, 0.985])
    plt.savefig(out_file, dpi=150)
    plt.close(fig)
    return counts


def load_values_from_state(_spark, local_json_path):
    with open(local_json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        return []

    values = []
    for row in payload:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_fare_amount/state")
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    state_key = f"{args.state_prefix}/current_rows.json"
    generated_key = f"{args.state_prefix}/last_generated_summary.json"
    snapshot_id = utc_now_compact()
    tmpdir = tempfile.mkdtemp(prefix="realtime_fare_hist_")
    state_file = os.path.join(tmpdir, "current_rows.json")
    chart_file = os.path.join(tmpdir, "inrange.png")
    summary_file = os.path.join(tmpdir, "summary.json")
    generated_file = os.path.join(tmpdir, "last_generated_summary.json")

    aws_cp_from_s3(args.bucket, state_key, state_file)

    spark = build_spark()
    values = load_values_from_state(spark, state_file)
    spark.stop()

    min_edge = 0
    max_value = max(values) if values else 1.0
    max_edge = int(math.ceil(max(max_value, 10.0) / 2.0) * 2)
    if max_edge <= min_edge:
        max_edge = min_edge + 2
    bin_edges = list(range(min_edge, max_edge + 2, 2))
    if len(bin_edges) < 2:
        bin_edges = [0, 2]

    counts = render_histogram(
        values,
        title="Realtime Fare Amount Histogram Demo",
        subtitle=f"snapshot={snapshot_id} | rows={len(values)} | values={values}",
        out_file=chart_file,
        bin_edges=bin_edges,
    )

    summary = {
        "feature": "fare_amount",
        "snapshot_id": snapshot_id,
        "row_count": len(values),
        "values": values,
        "bin_edges": bin_edges,
        "counts": counts,
        "state_key": state_key,
    }
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(generated_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "snapshot_id": snapshot_id,
                "row_count": len(values),
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "summary_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/summary.json",
            },
            f,
            indent=2,
        )

    base_key = f"{args.snapshot_prefix}/{snapshot_id}/fare_amount"
    aws_cp_to_s3(chart_file, args.bucket, f"{base_key}/inrange.png")
    aws_cp_to_s3(summary_file, args.bucket, f"{base_key}/summary.json")
    aws_cp_to_s3(state_file, args.bucket, f"{base_key}/source_rows.json")
    aws_cp_to_s3(generated_file, args.bucket, generated_key)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
