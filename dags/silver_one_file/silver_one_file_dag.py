from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"
SCRIPT_BASE = "/home/ubuntu/daihai_script/silver_one_file"
SPARK_COMMON = """
        --master yarn \
        --deploy-mode client \
        --driver-memory 2G \
        --executor-memory 2G \
        --executor-cores 2 \
        --conf spark.executor.instances=4 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.shuffle.partitions=32 \
        --conf spark.network.timeout=300s \
        --conf spark.sql.execution.arrow.enabled=false \
        --conf spark.sql.execution.arrow.pyspark.enabled=false \
        --conf spark.executor.memoryOverhead=512m \
        --conf spark.memory.offHeap.enabled=false \
        --conf spark.hadoop.fs.s3a.endpoint=http://192.168.100.66:9001 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=12345678 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
""".strip()

RAW_INPUT_PATH = "hdfs://namenode:9000/user/ubuntu/silver_one_file/input/yellow_tripdata_2025-01.parquet"
OUTPUT_BUCKET = "silver"
OUTPUT_KEY = "validation/silver_one_file/yellow_taxi_2025-01_clean.parquet"
TMP_OUTPUT_PREFIX = "validation/silver_one_file/_tmp"

default_args = {
    "owner": "you",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="silver_one_file",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    build_single_silver_file = BashOperator(
        task_id="build_single_silver_file",
        bash_command=f"""
        spark-submit {SPARK_COMMON} \
        {SCRIPT_BASE}/silver_one_file_job.py \
        --input-path {RAW_INPUT_PATH} \
        --output-bucket {OUTPUT_BUCKET} \
        --output-key {OUTPUT_KEY} \
        --tmp-output-prefix {TMP_OUTPUT_PREFIX}
""",
    )
