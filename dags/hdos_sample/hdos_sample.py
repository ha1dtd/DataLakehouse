from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime
import sys

SCRIPT_BASE = "/home/ubuntu/daihai_script/hdos_sample"
if SCRIPT_BASE not in sys.path:
    sys.path.insert(0, SCRIPT_BASE)

from foxai_config import (
    BRONZE_WAREHOUSE,
    GOLD_WAREHOUSE,
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
        --conf spark.executor.instances=4 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.shuffle.partitions=32 \
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

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

default_args = {
    "owner": "foxai",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "depends_on_past": False,
}

RAW_SCRIPT = f"{SCRIPT_BASE}/postgres_to_raw.py"
BRONZE_SCRIPT = f"{SCRIPT_BASE}/raw_to_bronze.py"
SILVER_SCRIPT = f"{SCRIPT_BASE}/bronze_to_silver.py"
GOLD_SCRIPT = f"{SCRIPT_BASE}/silver_to_gold.py"
CONFIG_FILE = f"{SCRIPT_BASE}/foxai_config.json"

with DAG(
    dag_id="hdos_sample",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    postgres_to_raw = BashOperator(
        task_id="postgres_to_raw",
        bash_command=f"""
        export FOXAI_CONFIG_FILE={CONFIG_FILE}
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages {ICEBERG_PACKAGES},{POSTGRES_JDBC_PACKAGE} \
        --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.raw_catalog.type=hadoop \
        --conf spark.sql.catalog.raw_catalog.warehouse={RAW_WAREHOUSE} \
        {S3A_CONF} \
        {RAW_SCRIPT}
"""
    )

    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=f"""
        export FOXAI_CONFIG_FILE={CONFIG_FILE}
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages {ICEBERG_PACKAGES} \
        --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.raw_catalog.type=hadoop \
        --conf spark.sql.catalog.raw_catalog.warehouse={RAW_WAREHOUSE} \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse={BRONZE_WAREHOUSE} \
        {S3A_CONF} \
        {BRONZE_SCRIPT}
"""
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"""
        export FOXAI_CONFIG_FILE={CONFIG_FILE}
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages {ICEBERG_PACKAGES} \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse={BRONZE_WAREHOUSE} \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse={SILVER_WAREHOUSE} \
        {S3A_CONF} \
        {SILVER_SCRIPT}
"""
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"""
        export FOXAI_CONFIG_FILE={CONFIG_FILE}
        {SPARK_SUBMIT_BIN} \
        {SPARK_COMMON} \
        --packages {ICEBERG_PACKAGES} \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse={SILVER_WAREHOUSE} \
        --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.gold_catalog.type=hadoop \
        --conf spark.sql.catalog.gold_catalog.warehouse={GOLD_WAREHOUSE} \
        {S3A_CONF} \
        {GOLD_SCRIPT}
"""
    )

    postgres_to_raw >> raw_to_bronze >> bronze_to_silver >> silver_to_gold
