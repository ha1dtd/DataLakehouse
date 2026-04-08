from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "depends_on_past": False
}

with DAG(
    dag_id="daihai_spark_hdfs_medallion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    silver = BashOperator(
        task_id="silver_layer",
        bash_command="""
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.yarn.jars="local:/opt/spark/jars/*" \
        --driver-memory 512M \
        --executor-memory 512M \
        --executor-cores 1 \
        --conf spark.yarn.am.memory=512M \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.executor.instances=1 \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9001 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        /home/anhtn/daihai_scripts/silver.py
    """
    )

    gold = BashOperator(
        task_id="gold_layer",
        bash_command="""
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.yarn.jars="local:/opt/spark/jars/*" \
        --driver-memory 512M \
        --executor-memory 512M \
        --executor-cores 1 \
        --conf spark.yarn.am.memory=512M \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.executor.instances=1 \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.silver_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.silver_catalog.type=hadoop \
        --conf spark.sql.catalog.silver_catalog.warehouse=s3a://silver/ \
        --conf spark.sql.catalog.gold_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.gold_catalog.type=hadoop \
        --conf spark.sql.catalog.gold_catalog.warehouse=s3a://gold/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9001 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        /home/anhtn/daihai_scripts/gold.py
    """
    )

    silver >> gold 
   #download_data >> create_bronze_dir >> upload_to_bronze >> bronze >> silver >> gold 
