from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from datetime import datetime

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"
KAFKA_BOOTSTRAP = "192.168.100.66:9092"
KAFKA_TOPIC = "raw_ingest_events"

S3A_CONF = f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"

SCRIPT_BASE = "/home/ubuntu/scripts"
DOMAIN_REGISTRY_FILE = f"{SCRIPT_BASE}/domain_registry_v2.json"

SPARK_COMMON = """
        --master yarn \
        --deploy-mode client \
        --driver-memory 2G \
        --executor-memory 6G \
        --executor-cores 5 \
        --conf spark.executor.instances=10 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.shuffle.partitions=200 \
        --conf spark.network.timeout=300s \
        --conf spark.sql.execution.arrow.enabled=false \
        --conf spark.sql.execution.arrow.pyspark.enabled=false \
        --conf spark.sql.iceberg.vectorization.enabled=false \
        --conf spark.executor.memoryOverhead=1G \
        --conf spark.yarn.executor.memoryOverhead=1G \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=1G \
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

with DAG(
    dag_id="combined_domain_medallion_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    enqueue_ingest_requests = BashOperator(
        task_id="enqueue_ingest_requests",
        bash_command=f"""
        export KAFKA_BOOTSTRAP={KAFKA_BOOTSTRAP}
        export KAFKA_TOPIC={KAFKA_TOPIC}
        export INGEST_SOURCES_FILE={SCRIPT_BASE}/ingest_sources_kafka_domains.json
        export DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE}
        python3 {SCRIPT_BASE}/kafka_enqueue_ingest_domains.py
"""
    )

    kafka_consume_to_raw = BashOperator(
        task_id="kafka_consume_to_raw",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 \
        --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.raw_catalog.type=hadoop \
        --conf spark.sql.catalog.raw_catalog.warehouse=s3a://raw/lakehouse/ \
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
        {SCRIPT_BASE}/kafka_consume_to_raw_domains.py
"""
    )

    bronze_from_raw = BashOperator(
        task_id="bronze_from_raw",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.databricks:spark-xml_2.12:0.18.0 \
        --conf spark.sql.catalog.raw_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.raw_catalog.type=hadoop \
        --conf spark.sql.catalog.raw_catalog.warehouse=s3a://raw/lakehouse/ \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/lakehouse/ \
        {S3A_CONF} \
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {SCRIPT_BASE}/bronze_from_raw_domains.py
"""
    )

    silver_from_bronze = BashOperator(
        task_id="silver_from_bronze",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        {SPARK_COMMON} \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.bronze_catalog.type=hadoop \
        --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/lakehouse/ \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
        {S3A_CONF} \
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {SCRIPT_BASE}/silver_from_bronze_domains.py
"""
    )

    gold_from_silver = BashOperator(
        task_id="gold_from_silver",
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
        --conf spark.yarn.appMasterEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        --conf spark.executorEnv.DOMAIN_REGISTRY_FILE={DOMAIN_REGISTRY_FILE} \
        {SCRIPT_BASE}/gold_from_silver_domains.py
"""
    )

    enqueue_ingest_requests >> kafka_consume_to_raw >> bronze_from_raw >> silver_from_bronze >> gold_from_silver
