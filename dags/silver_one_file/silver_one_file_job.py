import argparse
import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, unix_timestamp

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("SilverOneFile") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


def aws_cmd(*parts: str) -> list[str]:
    return [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} " + " ".join(parts)
        ),
    ]


def run_aws(*parts: str) -> subprocess.CompletedProcess:
    return subprocess.run(aws_cmd(*parts), check=True, capture_output=True, text=True)


def detect_yellow_schema(df) -> str:
    cols = set(df.columns)
    if "Trip_Pickup_DateTime" in cols:
        return "legacy"
    if "tpep_pickup_datetime" in cols:
        return "modern"
    return "intermediate"


def build_clean_df(input_path: str):
    raw = spark.read.parquet(input_path)
    schema_kind = detect_yellow_schema(raw)

    if schema_kind == "legacy":
        cleaned = raw.select(
            col("vendor_name").cast("string").alias("vendor_id"),
            col("Trip_Pickup_DateTime").cast("timestamp").alias("pickup_datetime"),
            col("Trip_Dropoff_DateTime").cast("timestamp").alias("dropoff_datetime"),
            col("Passenger_Count").cast("int").alias("passenger_count"),
            col("Trip_Distance").cast("double").alias("trip_distance"),
            col("Payment_Type").cast("string").alias("payment_type"),
            col("Fare_Amt").cast("double").alias("fare_amount"),
            col("Tip_Amt").cast("double").alias("tip_amount"),
            col("Tolls_Amt").cast("double").alias("tolls_amount"),
            col("Total_Amt").cast("double").alias("total_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            coalesce(col("surcharge"), lit(0.0)).cast("double").alias("surcharge"),
            col("Rate_Code").cast("string").alias("rate_code"),
            col("store_and_forward").cast("string").alias("store_and_fwd_flag"),
            col("Start_Lon").cast("double").alias("pickup_longitude"),
            col("Start_Lat").cast("double").alias("pickup_latitude"),
            col("End_Lon").cast("double").alias("dropoff_longitude"),
            col("End_Lat").cast("double").alias("dropoff_latitude"),
        )
    elif schema_kind == "modern":
        cleaned = raw.select(
            col("VendorID").cast("string").alias("vendor_id"),
            col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("payment_type").cast("string").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("total_amount").cast("double").alias("total_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            coalesce(col("extra"), lit(0.0)).cast("double").alias("surcharge"),
            col("RatecodeID").cast("string").alias("rate_code"),
            col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
            lit(None).cast("double").alias("pickup_longitude"),
            lit(None).cast("double").alias("pickup_latitude"),
            lit(None).cast("double").alias("dropoff_longitude"),
            lit(None).cast("double").alias("dropoff_latitude"),
        )
    else:
        cleaned = raw.select(
            col("vendor_id").cast("string").alias("vendor_id"),
            col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("payment_type").cast("string").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("total_amount").cast("double").alias("total_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            coalesce(col("surcharge"), lit(0.0)).cast("double").alias("surcharge"),
            col("rate_code").cast("string").alias("rate_code"),
            col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
            col("pickup_longitude").cast("double").alias("pickup_longitude"),
            col("pickup_latitude").cast("double").alias("pickup_latitude"),
            col("dropoff_longitude").cast("double").alias("dropoff_longitude"),
            col("dropoff_latitude").cast("double").alias("dropoff_latitude"),
        )

    cleaned = cleaned.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("trip_distance").isNotNull() &
        col("fare_amount").isNotNull() &
        col("tip_amount").isNotNull() &
        col("total_amount").isNotNull() &
        (col("fare_amount") >= 0) &
        (col("tip_amount") >= 0) &
        (col("total_amount") >= 0) &
        (col("trip_distance") >= 0) &
        (col("tolls_amount") >= 0)
    ).withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    ).filter(col("trip_duration_minutes") > 0) \
     .withColumn("domain", lit("validation")) \
     .withColumn("dataset", lit("yellow")) \
     .withColumn("silver_processed_ts", current_timestamp())

    return cleaned, schema_kind


def find_single_part_file(local_dir: Path) -> Path:
    matches = sorted(p for p in local_dir.rglob("part-*.parquet") if p.is_file())
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly 1 part parquet, found {len(matches)}")
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-bucket", required=True)
    parser.add_argument("--output-key", required=True)
    parser.add_argument("--tmp-output-prefix", required=True)
    args = parser.parse_args()

    cleaned, schema_kind = build_clean_df(args.input_path)
    input_count = spark.read.parquet(args.input_path).count()
    output_count = cleaned.count()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tmp_prefix = f"{args.tmp_output_prefix.rstrip('/')}/{run_id}"
    tmp_s3_path = f"s3a://{args.output_bucket}/{tmp_prefix}"

    logger.info("Reading raw file from %s", args.input_path)
    logger.info("Detected yellow schema kind=%s", schema_kind)
    logger.info("Input row count=%s | Output row count=%s", input_count, output_count)
    logger.info("Writing temporary single-part parquet to %s", tmp_s3_path)

    cleaned.coalesce(1).write.mode("overwrite").parquet(tmp_s3_path)

    local_tmpdir = Path(tempfile.mkdtemp(prefix="silver_one_file_"))
    try:
        run_aws("s3", "cp", f"s3://{args.output_bucket}/{tmp_prefix}/", str(local_tmpdir), "--recursive")
        part_file = find_single_part_file(local_tmpdir)
        final_local_file = local_tmpdir / Path(args.output_key).name
        shutil.copy2(part_file, final_local_file)

        run_aws("s3", "rm", f"s3://{args.output_bucket}/{args.output_key}") if False else None
        run_aws("s3", "cp", str(final_local_file), f"s3://{args.output_bucket}/{args.output_key}")
        run_aws("s3", "rm", f"s3://{args.output_bucket}/{tmp_prefix}/", "--recursive")

        summary = {
            "input_path": args.input_path,
            "output_bucket": args.output_bucket,
            "output_key": args.output_key,
            "tmp_output_prefix": tmp_prefix,
            "schema_kind": schema_kind,
            "input_row_count": input_count,
            "output_row_count": output_count,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        print(json.dumps(summary, indent=2))
    finally:
        shutil.rmtree(local_tmpdir, ignore_errors=True)
        spark.stop()


if __name__ == "__main__":
    main()
