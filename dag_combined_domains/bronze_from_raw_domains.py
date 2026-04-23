import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://192.168.100.66:9001")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "12345678")
ERROR_BUCKET = os.environ.get("ERROR_BUCKET", "error")
ERROR_PREFIX = os.environ.get("ERROR_PREFIX", "lakehouse/errors/bronze")
DOMAIN_REGISTRY_FILE = os.environ.get("DOMAIN_REGISTRY_FILE", "/home/ubuntu/scripts/domain_registry_v2.json")

spark = SparkSession.builder \
    .appName("BronzeFromRawDomains") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze_catalog.control")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bronze_catalog.control.raw_file_registry (
        job_id STRING,
        domain STRING,
        topic STRING,
        source_name STRING,
        source_uri STRING,
        raw_bucket STRING,
        raw_object_key STRING,
        bronze_bucket STRING,
        bronze_object_key STRING,
        file_name STRING,
        file_type STRING,
        bronze_format STRING,
        status STRING,
        error_message STRING,
        processed_ts TIMESTAMP
    ) USING iceberg
    """
)

with open(DOMAIN_REGISTRY_FILE, "r", encoding="utf-8") as f:
    domain_registry = json.load(f)

domains = domain_registry.get("domains", {})

raw_df = spark.read.table("raw_catalog.control.raw_registry") \
    .filter((col("status") == "landed") & (col("file_type").isin("json", "csv", "xml", "parquet")))

records = []

for row in raw_df.collect():
    domain_cfg = domains.get(row["domain"], {})
    bronze_bucket = domain_cfg.get("bronze_bucket", "bronze")
    bronze_prefix = domain_cfg.get("bronze_prefix", f"lakehouse/domains/{row['domain']}/bronze")

    raw_uri = f"s3a://{row['bucket']}/{row['object_key']}"
    base_name = os.path.splitext(row['file_name'])[0]
    bronze_object_key = f"{bronze_prefix}/{row['topic']}/{row['source_name']}/{base_name}.parquet"
    bronze_uri = f"s3a://{bronze_bucket}/{bronze_object_key}"
    status = "success"
    error_message = None

    try:
        if row["file_type"] == "json":
            df = spark.read.option("multiLine", "true").json(raw_uri)
        elif row["file_type"] == "csv":
            df = spark.read.option("header", "true").csv(raw_uri)
        elif row["file_type"] == "xml":
            df = spark.read.format("xml").option("rowTag", "order").load(raw_uri)
        elif row["file_type"] == "parquet":
            df = spark.read.parquet(raw_uri)
        else:
            raise ValueError(f"Unsupported file_type: {row['file_type']}")

        if df.rdd.isEmpty():
            raise ValueError("Parsed DataFrame is empty")

        df.write.mode("overwrite").parquet(bronze_uri)
    except Exception as e:
        status = "error"
        error_message = str(e)[:1000]
        try:
            jvm = spark.sparkContext._jvm
            conf = spark.sparkContext._jsc.hadoopConfiguration()
            src_path = jvm.org.apache.hadoop.fs.Path(raw_uri)
            err_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{ERROR_BUCKET}/{ERROR_PREFIX}/{row['domain']}/{row['topic']}/{row['file_name']}")
            src_fs = src_path.getFileSystem(conf)
            dst_fs = err_path.getFileSystem(conf)
            jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src_path, dst_fs, err_path, False, conf)
        except Exception:
            pass

    records.append((
        row["job_id"],
        row["domain"],
        row["topic"],
        row["source_name"],
        row["source_uri"],
        row["bucket"],
        row["object_key"],
        bronze_bucket,
        bronze_object_key,
        row["file_name"],
        row["file_type"],
        "parquet",
        status,
        error_message,
        row["ingest_ts"],
    ))

if records:
    out = spark.createDataFrame(records, schema="job_id string, domain string, topic string, source_name string, source_uri string, raw_bucket string, raw_object_key string, bronze_bucket string, bronze_object_key string, file_name string, file_type string, bronze_format string, status string, error_message string, processed_ts timestamp")
    out.writeTo("bronze_catalog.control.raw_file_registry").append()

print(f"BRONZE_PROCESSED={len(records)}")
