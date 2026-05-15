from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

S3A_CONF = f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"
SCRIPT_BASE = "/home/ubuntu/daihai_script/silver_sample_histogram"
DATA_BASE = "/home/ubuntu/daihai_script/data_test"
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
        --conf spark.memory.offHeap.enabled=false
""".strip()

default_args = {"owner": "you", "start_date": datetime(2024, 1, 1), "retries": 0, "depends_on_past": False}

with DAG(dag_id="fare_amount_realtime_demo", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    run_demo = BashOperator(
        task_id="generate_realtime_demo_histograms",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        {S3A_CONF} \
        {SCRIPT_BASE}/fare_amount_realtime_demo_job.py \
        --before-path {DATA_BASE}/fare_amount_realtime_5rows.json \
        --after-path {DATA_BASE}/fare_amount_realtime_6rows_after_ingest.json \
        --bucket histogram \
        --output-prefix test/realtime_fare_demo
"""
    )
