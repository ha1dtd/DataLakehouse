import argparse
import json
import math
import os
import subprocess
import tempfile
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"
MIN_EDGE = 0.0
HISTOGRAM_BINS = 40
QUANTILE_REL_ERR = 0.02


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


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def s3_to_s3a(path):
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


def build_spark():
    return (
        SparkSession.builder.appName("RealtimeValidateFareAmountCalculation")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def compute_two_stage_iqr_upper_bound_spark(cleaned_df):
    quantiles = cleaned_df.approxQuantile("fare_amount", [0.25, 0.75, 0.99], QUANTILE_REL_ERR)
    if len(quantiles) != 3 or any(value is None or math.isnan(value) for value in quantiles):
        stats = cleaned_df.agg(F.max("fare_amount").alias("max_value")).collect()[0]
        return max(float(stats["max_value"] or 10.0), 10.0)
    q25, q75, q99 = [float(value) for value in quantiles]
    iqr = q75 - q25
    hi = q99 if iqr <= 0 else min(q99, q75 + (3.0 * iqr))
    tail_df = cleaned_df.where(F.col("fare_amount") > F.lit(hi))
    tail_quantiles = tail_df.approxQuantile("fare_amount", [0.25, 0.75, 0.99], QUANTILE_REL_ERR)
    if len(tail_quantiles) == 3 and not any(value is None or math.isnan(value) for value in tail_quantiles):
        tq25, tq75, tq99 = [float(value) for value in tail_quantiles]
        tiqr = tq75 - tq25
        hi2 = tq99 if tiqr <= 0 else min(tq99, tq75 + (3.0 * tiqr))
        hi = max(hi, hi2)
    return max(float(hi), 10.0)


def build_bin_edges(upper_bound, bins=HISTOGRAM_BINS):
    span = max(float(upper_bound) - MIN_EDGE, 1.0)
    width = max(1.0, math.ceil(span / bins))
    max_edge = MIN_EDGE + (width * bins)
    bin_edges = [MIN_EDGE + (index * width) for index in range(bins + 1)]
    return bin_edges, float(width), float(max_edge)


def build_state_keys(state_prefix):
    return {
        "summary": f"{state_prefix}/last_ingest_summary.json",
        "file_state": f"{state_prefix}/file/current_file_event.json",
        "file_generated": f"{state_prefix}/file/last_generated_summary.json",
        "row_manifest": f"{state_prefix}/row/manifest.json",
        "row_generated": f"{state_prefix}/row/last_generated_summary.json",
        "generated": f"{state_prefix}/last_generated_summary.json",
    }


def build_histogram_from_cleaned_df(cleaned):
    row_count = int(cleaned.count())
    if row_count == 0:
        bin_edges, bin_width, max_edge = build_bin_edges(10.0)
        return row_count, bin_edges, [0 for _ in range(HISTOGRAM_BINS)], bin_width, 10.0, max_edge
    upper_bound = compute_two_stage_iqr_upper_bound_spark(cleaned)
    bin_edges, bin_width, max_edge = build_bin_edges(upper_bound)
    bucket_count = HISTOGRAM_BINS
    bucketed = cleaned.select(
        F.when(
            F.col("fare_amount") >= F.lit(upper_bound),
            F.lit(bucket_count - 1),
        )
        .otherwise(F.floor((F.col("fare_amount") - F.lit(MIN_EDGE)) / F.lit(bin_width)))
        .cast("int")
        .alias("bucket")
    )
    grouped = bucketed.groupBy("bucket").count().collect()
    count_map = {int(row["bucket"]): int(row["count"]) for row in grouped if row["bucket"] is not None}
    counts = [count_map.get(index, 0) for index in range(bucket_count)]
    return row_count, bin_edges, counts, bin_width, upper_bound, max_edge


def build_histogram_from_parquet(file_state, spark):
    source_path = s3_to_s3a(str(file_state.get("source_path") or ""))
    if not source_path:
        raise ValueError("Missing source_path in file state")
    df = spark.read.parquet(source_path).select(F.col("fare_amount").cast("double").alias("fare_amount"))
    cleaned = df.where(F.col("fare_amount").isNotNull())
    return build_histogram_from_cleaned_df(cleaned)


def build_histogram_from_row_manifest(bucket, manifest, spark):
    chunk_keys = manifest.get("chunk_keys") or []
    if not chunk_keys:
        bin_edges, bin_width, max_edge = build_bin_edges(10.0)
        return 0, bin_edges, [0 for _ in range(HISTOGRAM_BINS)], bin_width, 10.0, max_edge
    chunk_paths = [s3_to_s3a(f"s3://{bucket}/{chunk_key}") for chunk_key in chunk_keys]
    df = spark.read.json(chunk_paths).select(F.col("fare_amount").cast("double").alias("fare_amount"))
    cleaned = df.where(F.col("fare_amount").isNotNull())
    return build_histogram_from_cleaned_df(cleaned)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    state_keys = build_state_keys(args.state_prefix)
    snapshot_id = utc_now_compact()
    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_calc_")
    summary_file = os.path.join(tmpdir, "last_ingest_summary.json")
    file_state_file = os.path.join(tmpdir, "current_file_event.json")
    row_manifest_file = os.path.join(tmpdir, "row_manifest.json")
    calculation_file = os.path.join(tmpdir, "calculation_summary.json")
    comparison_file = os.path.join(tmpdir, "comparison.json")
    generated_file = os.path.join(tmpdir, "last_generated_summary.json")

    aws_cp_from_s3(args.bucket, state_keys["summary"], summary_file)
    ingest_summary = load_json(summary_file)
    mode = str(ingest_summary.get("mode") or "")
    if mode not in {"file", "row"}:
        raise ValueError(f"Unsupported calculation mode: {mode or 'missing'}")

    spark = build_spark()
    try:
        if mode == "row":
            aws_cp_from_s3(args.bucket, state_keys["row_manifest"], row_manifest_file)
            row_manifest = load_json(row_manifest_file)
            row_count, bin_edges, counts, bin_width, upper_bound, max_edge = build_histogram_from_row_manifest(args.bucket, row_manifest, spark)
            state_ref = state_keys["row_manifest"]
            source_path = row_manifest.get("source_path")
        else:
            aws_cp_from_s3(args.bucket, state_keys["file_state"], file_state_file)
            file_state = load_json(file_state_file)
            row_count, bin_edges, counts, bin_width, upper_bound, max_edge = build_histogram_from_parquet(file_state, spark)
            state_ref = state_keys["file_state"]
            source_path = file_state.get("source_path")
    finally:
        spark.stop()

    snapshot_id = f"{snapshot_id}_{mode}"
    summary = {
        "feature": "fare_amount",
        "mode": mode,
        "snapshot_id": snapshot_id,
        "row_count": row_count,
        "bin_width": bin_width,
        "upper_bound": upper_bound,
        "max_edge": max_edge,
        "bin_edges": bin_edges,
        "counts": counts,
        "state_key": state_ref,
        "source_path": source_path,
        "event_id": ingest_summary.get("event_id"),
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }
    comparison = {
        "feature": "fare_amount",
        "mode": mode,
        "snapshot_id": snapshot_id,
        "row_count": row_count,
        "bin_width": bin_width,
        "upper_bound": upper_bound,
        "max_edge": max_edge,
        "bin_edges": bin_edges,
        "counts": counts,
        "event_id": ingest_summary.get("event_id"),
        "source_path": source_path,
    }
    with open(calculation_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(comparison_file, "w", encoding="utf-8") as f:
        json.dump(comparison, f, indent=2)
    with open(generated_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "snapshot_id": snapshot_id,
                "mode": mode,
                "row_count": row_count,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "calculation_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/calculation/summary.json",
                "comparison_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/comparison.json",
                "summary_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/summary.json",
                "chart_key": f"{args.snapshot_prefix}/{snapshot_id}/fare_amount/inrange.png",
            },
            f,
            indent=2,
        )

    base_key = f"{args.snapshot_prefix}/{snapshot_id}/fare_amount"
    aws_cp_to_s3(calculation_file, args.bucket, f"{base_key}/calculation/summary.json")
    aws_cp_to_s3(comparison_file, args.bucket, f"{base_key}/comparison.json")
    aws_cp_to_s3(generated_file, args.bucket, state_keys["generated"])
    aws_cp_to_s3(
        generated_file,
        args.bucket,
        state_keys["file_generated"] if mode == "file" else state_keys["row_generated"],
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "feature": summary["feature"],
                "mode": summary["mode"],
                "snapshot_id": summary["snapshot_id"],
                "row_count": summary["row_count"],
                "bin_count": len(summary["counts"]),
                "comparison_key": f"{base_key}/comparison.json",
            }
        )
    )


if __name__ == "__main__":
    main()
