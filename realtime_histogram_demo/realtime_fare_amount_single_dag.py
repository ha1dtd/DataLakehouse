from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import ShortCircuitOperator  # type: ignore
from datetime import datetime
import json
import subprocess
import tempfile

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"
KAFKA_BOOTSTRAP = "192.168.100.66:9092"
KAFKA_TOPIC = "realtime_fare_amount_demo"
KAFKA_GROUP_ID = "realtime-fare-amount-demo-airflow"

S3A_CONF = f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"
SCRIPT_BASE = "/home/ubuntu/daihai_script/realtime_histogram_demo"
SPARK_COMMON = """
        --master yarn \
        --deploy-mode client \
        --driver-memory 2G \
        --executor-memory 4G \
        --executor-cores 2 \
        --conf spark.executor.instances=2 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.network.timeout=300s \
        --conf spark.sql.execution.arrow.enabled=false \
        --conf spark.sql.execution.arrow.pyspark.enabled=false \
        --conf spark.executor.memoryOverhead=1G \
        --conf spark.yarn.executor.memoryOverhead=1G
""".strip()
PYTHON_COMMON = f"export KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP} && export KAFKA_TOPIC={KAFKA_TOPIC} && export KAFKA_GROUP_ID={KAFKA_GROUP_ID}"


def should_generate_histogram():
    summary_key = "demo/realtime_fare_amount/state/last_consume_summary.json"
    current_rows_key = "demo/realtime_fare_amount/state/current_rows.json"
    generated_key = "demo/realtime_fare_amount/state/last_generated_summary.json"
    tmpdir = tempfile.mkdtemp(prefix="realtime_fare_gate_")
    summary_file = f"{tmpdir}/last_consume_summary.json"
    current_rows_file = f"{tmpdir}/current_rows.json"
    generated_file = f"{tmpdir}/last_generated_summary.json"

    def s3_cp(key, local_file, required=True):
        cmd = [
            "bash",
            "-lc",
            (
                f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
                f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
                f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp s3://histogram/{key} '{local_file}' >/dev/null 2>&1"
            ),
        ]
        result = subprocess.run(cmd)
        if required and result.returncode != 0:
            raise RuntimeError(f"Missing required state file: {key}")
        return result.returncode == 0

    s3_cp(summary_key, summary_file, required=True)
    s3_cp(current_rows_key, current_rows_file, required=True)
    generated_exists = s3_cp(generated_key, generated_file, required=False)

    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)
    with open(current_rows_file, "r", encoding="utf-8") as f:
        current_rows = json.load(f)

    current_row_count = len(current_rows) if isinstance(current_rows, list) else 0
    generated_row_count = 0
    generated_summary_key = ""
    if generated_exists:
        with open(generated_file, "r", encoding="utf-8") as f:
            generated_summary = json.load(f)
        generated_row_count = int(generated_summary.get("row_count") or 0)
        generated_summary_key = str(generated_summary.get("summary_key") or "")

    generated_marker_is_legacy = generated_exists and (
        generated_summary_key.startswith("test/")
        or generated_summary_key.startswith("demo/realtime_fare_amount/")
        or "/yellow_taxi/" in generated_summary_key
    )

    return (
        bool(summary.get("should_generate_histogram"))
        or current_row_count > generated_row_count
        or generated_marker_is_legacy
    )


default_args = {"owner": "you", "start_date": datetime(2024, 1, 1), "retries": 0, "depends_on_past": False}

with DAG(
    dag_id="realtime_fare_amount_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    consume_and_update = BashOperator(
        task_id="consume_kafka_and_update_minio_state",
        bash_command=f"""
        {PYTHON_COMMON} && \
        python3 {SCRIPT_BASE}/realtime_fare_amount_kafka_consume_and_update_v2.py \
        --bucket histogram \
        --state-prefix demo/realtime_fare_amount/state \
        --max-messages 100 \
        --timeout-ms 3000
""",
    )

    gate_histogram = ShortCircuitOperator(
        task_id="gate_histogram_if_new_data",
        python_callable=should_generate_histogram,
    )

    generate_histogram = BashOperator(
        task_id="generate_histogram_snapshot",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        {S3A_CONF} \
        {SCRIPT_BASE}/realtime_fare_amount_histogram_job.py \
        --bucket histogram \
        --state-prefix demo/realtime_fare_amount/state \
        --snapshot-prefix demo
""",
    )

    consume_and_update >> gate_histogram >> generate_histogram
