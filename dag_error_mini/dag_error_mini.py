from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

S3A_CONF = f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"

SCRIPT_BASE = "/home/ubuntu/scripts"

SPARK_COMMON = """
        --master yarn \\
        --deploy-mode client \\
        --driver-memory 2G \\
        --executor-memory 6G \\
        --executor-cores 5 \\
        --conf spark.executor.instances=10 \\
        --conf spark.dynamicAllocation.enabled=false \\
        --conf spark.sql.shuffle.partitions=200 \\
        --conf spark.network.timeout=300s \\
        --conf spark.sql.execution.arrow.enabled=false \\
        --conf spark.sql.execution.arrow.pyspark.enabled=false \\
        --conf spark.sql.iceberg.vectorization.enabled=false \\
        --conf spark.executor.memoryOverhead=1G \\
        --conf spark.yarn.executor.memoryOverhead=1G \\
        --conf spark.memory.offHeap.enabled=true \\
        --conf spark.memory.offHeap.size=1G \\
        --conf spark.sql.files.maxPartitionBytes=134217728 \\
        --conf spark.executor.extraJavaOptions=\"-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED\" \\
        --conf spark.driver.extraJavaOptions=\"-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true\" \\
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.adaptive.enabled=true \\
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
        --conf spark.sql.adaptive.skewJoin.enabled=true \\
        --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB \
""".strip()

default_args = {
    "owner": "you",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "depends_on_past": False,
}

with DAG(
    dag_id="spark_minio_transform_pipeline_error_mini",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    read_bronze_error_mini = BashOperator(
        task_id="read_bronze_error_mini",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/ \
        {S3A_CONF} \
        {SCRIPT_BASE}/read_bronze_error_mini.py
"""
    )

    bronze_to_silver_error_mini = BashOperator(
        task_id="bronze_to_silver_error_mini",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/ \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
        {S3A_CONF} \
        {SCRIPT_BASE}/silver_error_mini.py
"""
    )

    silver_to_gold_error_mini = BashOperator(
        task_id="silver_to_gold_error_mini",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
        --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.gold_catalog.type=hadoop \
        --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/lakehouse/ \
        {S3A_CONF} \
        {SCRIPT_BASE}/gold_error_mini.py
"""
    )

    read_bronze_error_mini >> bronze_to_silver_error_mini >> silver_to_gold_error_mini
