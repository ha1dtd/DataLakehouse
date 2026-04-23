from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

S3A_CONF = f"--conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false"

default_args = {
 "owner": "you",
 "start_date": datetime(2024, 1, 1),
 "retries": 1,
 "depends_on_past": False
}
with DAG(
 dag_id="spark_minio_medallion_pipeline",
 default_args=default_args,
 schedule_interval="@hourly",
 catchup=False
) as dag:

 # 1. Setup: Upload NameNode local dataset to MinIO Raw
 ingestion = BashOperator(
 task_id="file_setup",
 env={
 "AWS_ACCESS_KEY_ID": "admin",
 "AWS_SECRET_ACCESS_KEY": "12345678",
 "AWS_ENDPOINT_URL": "http://192.168.100.66:9001"
 },
 bash_command=f"""
 aws --endpoint-url $AWS_ENDPOINT_URL s3 cp /home/ubuntu/data_70gb/ s3://bronze/raw/ --recursive
 """
 )

 # 2. Bronze to Silver
 bronze_to_silver = BashOperator(
 task_id="bronze_to_silver",
 bash_command=f"""
 /opt/spark/bin/spark-submit \
 --master yarn \
 --deploy-mode client \
 --driver-memory 1G \
 --executor-memory 2G \
 --executor-cores 1 \
 --conf spark.executor.instances=1 \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.sql.shuffle.partitions=2 \
 --conf spark.network.timeout=300s \
 --conf spark.sql.execution.arrow.enabled=false \
 --conf spark.sql.execution.arrow.pyspark.enabled=false \
 --conf spark.sql.iceberg.vectorization.enabled=false \
 --conf spark.executor.memoryOverhead=1024 \
 --conf spark.yarn.executor.memoryOverhead=1024 \
 --conf spark.memory.offHeap.enabled=true \
 --conf spark.memory.offHeap.size=1G \
 --conf spark.executor.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
 --conf spark.driver.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true" \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.bronze_catalog.type=hadoop \
 --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/ \
 --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.silver_catalog.type=hadoop \
 --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
 {S3A_CONF} \
 /home/ubuntu/scripts/bronze_to_silver.py
 """
 )

 # 3. Silver to Gold
 # --conf spark.yarn.jars=local:/opt/spark/jars/* \
 silver_to_gold = BashOperator(
 task_id="silver_to_gold",
 bash_command=f"""
 /opt/spark/bin/spark-submit \
 --master yarn \
 --deploy-mode client \
 --driver-memory 1G \
 --executor-memory 2G \
 --executor-cores 1 \
 --conf spark.executor.instances=1 \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.sql.shuffle.partitions=2 \
 --conf spark.network.timeout=300s \
 --conf spark.sql.execution.arrow.enabled=false \
 --conf spark.sql.execution.arrow.pyspark.enabled=false \
 --conf spark.sql.iceberg.vectorization.enabled=false \
 --conf spark.executor.memoryOverhead=1024 \
 --conf spark.yarn.executor.memoryOverhead=1024 \
 --conf spark.memory.offHeap.enabled=true \
 --conf spark.memory.offHeap.size=1G \
 --conf spark.executor.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
 --conf spark.driver.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true" \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.silver_catalog.type=hadoop \
 --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/lakehouse/ \
 --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.gold_catalog.type=hadoop \
 --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/lakehouse/ \
 {S3A_CONF} \
 /home/ubuntu/scripts/silver_to_gold.py
 """
 )

 # 4. Validation/Reading tasks
 read_bronze = BashOperator(
 task_id="read_bronze",
 bash_command=f"""
 /opt/spark/bin/spark-submit \
 --master yarn \
 --deploy-mode client \
 --driver-memory 1G \
 --executor-memory 2G \
 --executor-cores 1 \
 --conf spark.executor.instances=1 \
 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
 --conf spark.dynamicAllocation.enabled=false \
 --conf spark.sql.shuffle.partitions=2 \
 --conf spark.network.timeout=300s \
 --conf spark.sql.execution.arrow.enabled=false \
 --conf spark.sql.execution.arrow.pyspark.enabled=false \
 --conf spark.sql.iceberg.vectorization.enabled=false \
 --conf spark.executor.memoryOverhead=1024 \
 --conf spark.yarn.executor.memoryOverhead=1024 \
 --conf spark.memory.offHeap.enabled=true \
 --conf spark.memory.offHeap.size=1G \
 --conf spark.executor.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
 --conf spark.driver.extraJavaOptions="-Dio.netty.transport.noNative=true -Dio.netty.handler.ssl.noOpenSsl=true -Dorg.wildfly.openssl.disable=true" \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
 --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.bronze_catalog.type=hadoop \
 --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/ \
 {S3A_CONF} \
 /home/ubuntu/scripts/read_bronze.py
 """
 )

 ingestion >> read_bronze >> bronze_to_silver >> silver_to_gold
 #download_data >> create_bronze_dir >> upload_to_bronze >> bronze >> silver >> gold