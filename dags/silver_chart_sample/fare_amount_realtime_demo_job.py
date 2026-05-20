import argparse
import json
import math
import os
import tempfile

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
        .appName("FareAmountRealtimeDemo") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def format_thousands_dot(v, _pos=None):
    try:
        return f"{int(round(v)):,}".replace(",", ".")
    except Exception:
        return str(v)


def upload_with_aws(local_file, bucket, key):
    cmd = (
        f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
        f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
        f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp '{local_file}' s3://{bucket}/{key}"
    )
    rc = os.system(cmd)
    if rc != 0:
        raise RuntimeError(f"upload failed: {cmd}")


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


def load_values(spark, path):
    df = spark.read.option("multiLine", True).json(path)
    cleaned = (
        df.select(F.col("fare_amount").cast("double").alias("fare_amount"))
        .where(F.col("fare_amount").isNotNull())
        .orderBy(F.col("fare_amount"))
    )
    return [float(row["fare_amount"]) for row in cleaned.collect()]


def write_summary(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before-path", required=True)
    parser.add_argument("--after-path", required=True)
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--output-prefix", default="test/realtime_fare_demo")
    args = parser.parse_args()

    spark = build_spark()
    tmpdir = tempfile.mkdtemp(prefix="fare_amount_realtime_demo_")

    before_values = load_values(spark, args.before_path)
    after_values = load_values(spark, args.after_path)
    spark.stop()

    all_values = before_values + after_values
    min_edge = 0
    max_value = max(all_values) if all_values else 1.0
    max_edge = int(math.ceil(max(max_value, 10.0) / 2.0) * 2)
    if max_edge <= min_edge:
        max_edge = min_edge + 2
    bin_edges = list(range(min_edge, max_edge + 2, 2))
    if len(bin_edges) < 2:
        bin_edges = [0, 2]

    before_file = os.path.join(tmpdir, "before_ingest.png")
    after_file = os.path.join(tmpdir, "after_ingest.png")
    summary_file = os.path.join(tmpdir, "summary.json")

    before_counts = render_histogram(
        before_values,
        title="Fare Amount Realtime Demo - Before New Row",
        subtitle=f"rows={len(before_values)} | values={before_values}",
        out_file=before_file,
        bin_edges=bin_edges,
    )
    after_counts = render_histogram(
        after_values,
        title="Fare Amount Realtime Demo - After New Row",
        subtitle=f"rows={len(after_values)} | values={after_values}",
        out_file=after_file,
        bin_edges=bin_edges,
    )

    summary = {
        "feature": "fare_amount",
        "bin_edges": bin_edges,
        "before": {
            "rows": len(before_values),
            "values": before_values,
            "counts": before_counts,
        },
        "after": {
            "rows": len(after_values),
            "values": after_values,
            "counts": after_counts,
            "new_row": 15.0,
        },
    }
    write_summary(summary_file, summary)

    upload_with_aws(before_file, args.bucket, f"{args.output_prefix}/before_ingest.png")
    upload_with_aws(after_file, args.bucket, f"{args.output_prefix}/after_ingest.png")
    upload_with_aws(summary_file, args.bucket, f"{args.output_prefix}/summary.json")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
