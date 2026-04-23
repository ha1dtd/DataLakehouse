import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.request import urlopen

from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "192.168.100.66:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_ingest_events")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/opt/confluent/bin")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "100"))
RAW_BUCKET = os.environ.get("RAW_BUCKET", "raw")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://192.168.100.66:9001")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "12345678")
ERROR_BUCKET = os.environ.get("ERROR_BUCKET", "error")
ERROR_PREFIX = os.environ.get("ERROR_PREFIX", "raw_ingest_kafka")

spark = SparkSession.builder \
    .appName("KafkaConsumeToRaw") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS raw_catalog.default")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS raw_catalog.default.raw_registry (
        job_id STRING,
        source_type STRING,
        source_name STRING,
        source_uri STRING,
        bucket STRING,
        object_key STRING,
        file_name STRING,
        file_type STRING,
        file_size_bytes BIGINT,
        ingest_ts TIMESTAMP,
        status STRING,
        error_message STRING
    ) USING iceberg
    """
)


def utc_now():
    return datetime.now(timezone.utc)


def fetch_to_local(source_type: str, source_uri: str, target_path: str):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    if source_type in ("vm_file", "local_file"):
        shutil.copyfile(source_uri, target_path)
    elif source_type == "url":
        with urlopen(source_uri) as r, open(target_path, "wb") as f:
            shutil.copyfileobj(r, f)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")


def read_messages_from_kafka_cli(limit: int):
    cmd = [
        f"{KAFKA_BIN}/kafka-console-consumer",
        "--bootstrap-server",
        KAFKA_BOOTSTRAP,
        "--topic",
        KAFKA_TOPIC,
        "--from-beginning",
        "--timeout-ms",
        "5000",
        "--max-messages",
        str(limit),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    messages = []
    for line in lines:
        try:
            payload = json.loads(line)
            if payload.get("event_type") == "raw_ingest_request":
                messages.append(payload)
        except json.JSONDecodeError:
            continue
    return messages


records = []
messages = read_messages_from_kafka_cli(MAX_MESSAGES)
processed = 0

for payload in messages:
    job_id = payload.get("job_id")
    source_type = payload.get("source_type")
    source_uri = payload.get("source_uri")
    source_name = payload.get("source_name") or "unknown_source"
    file_name = payload.get("file_name") or os.path.basename(urlparse(source_uri).path)
    file_type = payload.get("file_type") or os.path.splitext(file_name)[1].lstrip(".").lower() or "unknown"
    local_tmp = f"/tmp/{job_id}_{file_name}"
    object_key = f"{source_name}/{file_name}"
    target_uri = f"s3a://{RAW_BUCKET}/{object_key}"
    status = "landed"
    error_message = None
    file_size_bytes = None

    try:
        fetch_to_local(source_type, source_uri, local_tmp)
        file_size_bytes = os.path.getsize(local_tmp)
        jvm = spark.sparkContext._jvm
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        local_path = jvm.org.apache.hadoop.fs.Path(f"file://{local_tmp}")
        dst_path = jvm.org.apache.hadoop.fs.Path(target_uri)
        dst_fs = dst_path.getFileSystem(conf)
        dst_fs.copyFromLocalFile(False, True, local_path, dst_path)
    except Exception as e:
        status = "error"
        error_message = str(e)[:1000]
        try:
            if os.path.exists(local_tmp):
                jvm = spark.sparkContext._jvm
                conf = spark.sparkContext._jsc.hadoopConfiguration()
                local_path = jvm.org.apache.hadoop.fs.Path(f"file://{local_tmp}")
                err_path = jvm.org.apache.hadoop.fs.Path(f"s3a://{ERROR_BUCKET}/{ERROR_PREFIX}/{file_name}")
                err_fs = err_path.getFileSystem(conf)
                err_fs.copyFromLocalFile(False, True, local_path, err_path)
        except Exception:
            pass
    finally:
        if os.path.exists(local_tmp):
            os.remove(local_tmp)

    records.append((
        job_id,
        source_type,
        source_name,
        source_uri,
        RAW_BUCKET,
        object_key,
        file_name,
        file_type,
        file_size_bytes,
        utc_now(),
        status,
        error_message,
    ))
    processed += 1

if records:
    df = spark.createDataFrame(records, schema="job_id string, source_type string, source_name string, source_uri string, bucket string, object_key string, file_name string, file_type string, file_size_bytes long, ingest_ts timestamp, status string, error_message string")
    df.writeTo("raw_catalog.default.raw_registry").append()

print(f"PROCESSED_MESSAGES={processed}")
