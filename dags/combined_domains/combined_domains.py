from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime
from pathlib import Path
import sys

SCRIPT_BASE = "/home/ubuntu/daihai_script/dag_combined_domains"
if SCRIPT_BASE not in sys.path:
    sys.path.insert(0, SCRIPT_BASE)

from dags.combined_domains.foxai_config import (
    BRONZE_WAREHOUSE,
    DOMAIN_REGISTRY_FILE,
    GOLD_WAREHOUSE,
    INGEST_SOURCES_FILE,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RAW_WAREHOUSE,
    SCRIPT_BASE,
    SILVER_WAREHOUSE,
    SPARK_SUBMIT_BIN,
)

S3A_PATH_STYLE_ACCESS = "true"
S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem"
S3A_SSL_ENABLED = "false"
S3A_CONF = (
    f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} "
    f"--conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} "
    f"--conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} "
    f"--conf spark.hadoop.fs.s3a.path.style.access={S3A_PATH_STYLE_ACCESS} "
    f"--conf spark.hadoop.fs.s3a.impl={S3A_IMPL} "
    f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled={S3A_SSL_ENABLED}"
)

SPARK_COMMON = """
        --master yarn \
        --deploy-mode client \
        --driver-memory 2G \
        --executor-memory 2G \
        --executor-cores 2 \
        --conf spark.executor.instances=8 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.shuffle.partitions=64 \
        --conf spark.network.timeout=300s \
        --conf spark.sql.execution.arrow.enabled=false \
        --conf spark.sql.execution.arrow.pyspark.enabled=false \
        --conf spark.sql.iceberg.vectorization.enabled=false \
        --conf spark.executor.memoryOverhead=512m \
        --conf spark.memory.offHeap.enabled=false \
        --conf spark.sql.files.maxPartitionBytes=134217728 \
        --conf spark.executor.extraJavaOptions=\"-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" \
        --conf spark.driver.extraJavaOptions=\"-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true\" \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.adaptive.skewJoin.enabled=true \
        --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB
""".strip()

default_args = {
    "owner": "you",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "depends_on_past": False,
}

KAFKA_ENQUEUE_SCRIPT = f"{SCRIPT_BASE}/kafka_enqueue_ingest_domains.py"
KAFKA_CONSUME_SCRIPT = f"{SCRIPT_BASE}/kafka_consume_to_raw_domains.py"
BRONZE_SCRIPT = f"{SCRIPT_BASE}/bronze_from_raw_domains.py"
SILVER_SCRIPT = f"{SCRIPT_BASE}/silver_from_bronze_domains.py"
GOLD_SCRIPT = f"{SCRIPT_BASE}/gold_from_silver_domains.py"

with DAG(
    dag_id="combined_domains",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    enqueue_ingest_requests = BashOperator(
        task_id="enqueue_ingest_requests",
        bash_command=f"""
        export KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}
        export KAFKA_TOPIC={KAFKA_TOPIC}
        export INGEST_SOURCES_FILE={INGEST_SOURCES_FILE}
        export DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE}
        python3 {KAFKA_ENQUEUE_SCRIPT}
"""
    )

    kafka_consume_to_raw = BashOperator(
        task_id="kafka_consume_to_raw",
        bash_command=f"""
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 \
        --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.raw_catalog.type=hadoop \
        --conf spark.sql.catalog.raw_catalog.warehouse={RAW_WAREHOUSE} \
        {S3A_CONF} \
        --conf spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP} \
        --conf spark.executorEnv.KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP} \
        --conf spark.yarn.appMasterEnv.KAFKA_TOPIC={KAFKA_TOPIC} \
        --conf spark.executorEnv.KAFKA_TOPIC={KAFKA_TOPIC} \
        --conf spark.yarn.appMasterEnv.MINIO_ENDPOINT={MINIO_ENDPOINT} \
        --conf spark.executorEnv.MINIO_ENDPOINT={MINIO_ENDPOINT} \
        --conf spark.yarn.appMasterEnv.MINIO_ACCESS_KEY={MINIO_ACCESS_KEY} \
        --conf spark.executorEnv.MINIO_ACCESS_KEY={MINIO_ACCESS_KEY} \
        --conf spark.yarn.appMasterEnv.MINIO_SECRET_KEY={MINIO_SECRET_KEY} \
        --conf spark.executorEnv.MINIO_SECRET_KEY={MINIO_SECRET_KEY} \
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {KAFKA_CONSUME_SCRIPT}
"""
    )

    bronze_from_raw = BashOperator(
        task_id="bronze_from_raw",
        bash_command=f"""
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.databricks:spark-xml_2.12:0.18.0 \
        --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.raw_catalog.type=hadoop \
        --conf spark.sql.catalog.raw_catalog.warehouse={RAW_WAREHOUSE} \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse={BRONZE_WAREHOUSE} \
        {S3A_CONF} \
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {BRONZE_SCRIPT}
"""
    )

    silver_from_bronze = BashOperator(
        task_id="silver_from_bronze",
        bash_command=f"""
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse={BRONZE_WAREHOUSE} \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse={SILVER_WAREHOUSE} \
        {S3A_CONF} \
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {SILVER_SCRIPT}
"""
    )

    gold_from_silver = BashOperator(
        task_id="gold_from_silver",
        bash_command=f"""
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse={SILVER_WAREHOUSE} \
        --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.gold_catalog.type=hadoop \
        --conf spark.sql.catalog.gold_catalog.warehouse={GOLD_WAREHOUSE} \
        {S3A_CONF} \
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {GOLD_SCRIPT}
"""
    )

    enqueue_ingest_requests >> kafka_consume_to_raw >> bronze_from_raw >> silver_from_bronze >> gold_from_silver
