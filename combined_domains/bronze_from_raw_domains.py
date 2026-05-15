import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from foxai_config import DOMAIN_REGISTRY_FILE, ERROR_BUCKET, ERROR_PREFIX_BRONZE, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

ERROR_PREFIX = ERROR_PREFIX_BRONZE


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

raw_df = spark.read.table("raw_catalog.registry.raw_registry") \
    .filter((col("status") == "landed") & (col("file_type").isin("json", "csv", "xml", "parquet")))

processed_success_df = spark.read.table("bronze_catalog.control.raw_file_registry") \
    .filter(col("status") == "success") \
    .select("domain", "topic", "source_name", "source_uri", "raw_bucket", "raw_object_key", "file_name") \
    .distinct()

candidate_df = raw_df.alias("r").join(
    processed_success_df.alias("p"),
    on=[
        col("r.domain") == col("p.domain"),
        col("r.topic") == col("p.topic"),
        col("r.source_name") == col("p.source_name"),
        col("r.source_uri") == col("p.source_uri"),
        col("r.bucket") == col("p.raw_bucket"),
        col("r.object_key") == col("p.raw_object_key"),
        col("r.file_name") == col("p.file_name"),
    ],
    how="left_anti"
)

logger.info("BRONZE_PHASE=start")
records = []
raw_total = raw_df.count()
rows = candidate_df.collect()
skipped_already_bronzed = raw_total - len(rows)
logger.info("BRONZE_RAW_INPUT_COUNT=%s", raw_total)
logger.info("BRONZE_CANDIDATE_FILES=%s", len(rows))
logger.info("BRONZE_TARGET_REGISTRY=%s", "bronze_catalog.control.raw_file_registry")

for row in rows:
    domain_cfg = domains.get(row["domain"], {})
    bronze_bucket = domain_cfg.get("bronze_bucket", "bronze")
    bronze_prefix = domain_cfg.get("bronze_prefix", f"lakehouse/{row['domain']}")

    raw_uri = f"s3a://{row['bucket']}/{row['object_key']}"
    base_name = os.path.splitext(row['file_name'])[0]
    bronze_object_key = f"{bronze_prefix}/{base_name}.parquet"
    bronze_uri = f"s3a://{bronze_bucket}/{bronze_object_key}"
    status = "success"
    error_message = None

    try:
        logger.info(
            "BRONZE_FILE_START domain=%s topic=%s file=%s source=%s target=%s",
            row["domain"],
            row["topic"],
            row["file_name"],
            raw_uri,
            bronze_uri,
        )
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
        logger.info(
            "BRONZE_FILE_DONE domain=%s topic=%s file=%s target=%s",
            row["domain"],
            row["topic"],
            row["file_name"],
            bronze_uri,
        )
    except Exception as e:
        status = "error"
        error_message = str(e)[:1000]
        logger.exception(
            "BRONZE_FILE_FAILED domain=%s topic=%s file=%s source=%s target=%s error=%s",
            row["domain"],
            row["topic"],
            row["file_name"],
            raw_uri,
            bronze_uri,
            error_message,
        )
        try:
            jvm = spark.sparkContext._jvm
            conf = spark.sparkContext._jsc.hadoopConfiguration()
            src_path = jvm.org.apache.hadoop.fs.Path(raw_uri)
            err_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{ERROR_BUCKET}/{ERROR_PREFIX}/{row['domain']}/{row['topic']}/{row['file_name']}")
            src_fs = src_path.getFileSystem(conf)
            dst_fs = err_path.getFileSystem(conf)
            jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src_path, dst_fs, err_path, False, conf)
        except Exception:
            logger.exception(
                "BRONZE_ERROR_COPY_FAILED domain=%s topic=%s file=%s error_target=%s",
                row["domain"],
                row["topic"],
                row["file_name"],
                f"s3a://{ERROR_BUCKET}/{ERROR_PREFIX}/{row['domain']}/{row['topic']}/{row['file_name']}",
            )

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

success_count = sum(1 for record in records if record[12] == "success")
error_count = sum(1 for record in records if record[12] == "error")
logger.info("BRONZE_PROCESSED=%s", len(records))
logger.info("BRONZE_SUCCESS=%s", success_count)
logger.info("BRONZE_ERROR=%s", error_count)
logger.info("SKIPPED_ALREADY_BRONZED=%s", skipped_already_bronzed)
logger.info("BRONZE_PHASE=end")
