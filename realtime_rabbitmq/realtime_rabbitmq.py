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

S3A_CONF = f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"
SCRIPT_BASE = "/home/ubuntu/daihai_script/realtime_rabbitmq"
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
STATE_PREFIX = "demo/realtime_rabbitmq_fare_amount/state"
SNAPSHOT_PREFIX = "demo"


def should_continue():
    summary_key = f"{STATE_PREFIX}/last_ingest_summary.json"
    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_gate_")
    summary_file = f"{tmpdir}/last_ingest_summary.json"

    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp s3://histogram/{summary_key} '{summary_file}' >/dev/null 2>&1"
        ),
    ]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"Missing required state file: {summary_key}")

    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)
    mode = str(summary.get("mode") or "")
    snapshot_label = str(summary.get("snapshot_label") or "")
    if mode not in {"file", "row"}:
        raise RuntimeError(f"Unsupported ingest mode in summary: {mode or 'missing'}")
    if not snapshot_label:
        raise RuntimeError("Missing snapshot_label in last_ingest_summary")
    return bool(summary.get("should_calculate"))


default_args = {"owner": "you", "start_date": datetime(2024, 1, 1), "retries": 0, "depends_on_past": False}

with DAG(
    dag_id="realtime_rabbitmq",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    ingest_event = BashOperator(
        task_id="ingest_rabbitmq_event_to_minio_state",
        bash_command="""
        EVENT_KEY='{{ dag_run.conf.get("event_key") }}' && \
        python3 /home/ubuntu/daihai_script/realtime_rabbitmq/realtime_fare_amount_rabbitmq_ingest_event.py \
        --bucket histogram \
        --state-prefix demo/realtime_rabbitmq_fare_amount/state \
        --snapshot-prefix demo \
        --event-key "$EVENT_KEY"
""",
    )

    gate_calculation = ShortCircuitOperator(
        task_id="gate_calculation_if_new_data",
        python_callable=should_continue,
    )

    calculate = BashOperator(
        task_id="calculate_histogram_data",
        bash_command=f"""
        python3 {SCRIPT_BASE}/realtime_fare_amount_rabbitmq_calculation_job.py \
        --bucket histogram \
        --state-prefix {STATE_PREFIX} \
        --snapshot-prefix {SNAPSHOT_PREFIX}
""",
    )

    generate_chart = BashOperator(
        task_id="generate_histogram_snapshot",
        bash_command=f"""
        python3 {SCRIPT_BASE}/realtime_fare_amount_histogram_job.py \
        --bucket histogram \
        --state-prefix {STATE_PREFIX} \
        --snapshot-prefix {SNAPSHOT_PREFIX}
""",
    )

    ingest_event >> gate_calculation >> calculate >> generate_chart
