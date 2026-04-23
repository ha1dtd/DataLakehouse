from misc.airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime


def other_tasks():
     # # for i in $(seq -w 1 11) to download the full year of files
    # download_data = BashOperator(
    #     task_id="download_data",
    #     bash_command="""
    #     mkdir -p /home/n3cr0d3m0nncrdmn/data

    #     for i in $(seq -w 1 2)
    #     do
    #         wget --user-agent="Mozilla/5.0" -nc -P /home/n3cr0d3m0nncrdmn/data \
    #         https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-$i.parquet
    #     done
    #     """
    # )

    # create_bronze_dir = BashOperator(
    #     task_id="create_bronze_dir",
    #     bash_command="hdfs dfs -mkdir -p /data/bronze"
    # )

    # upload_to_bronze = BashOperator(
    #     task_id="upload_to_bronze",
    #     bash_command="""
    #     for i in $(seq -w 1 2)
    #     do
    #         hdfs dfs -put -f /home/n3cr0d3m0nncrdmn/data/yellow_tripdata_2025-$i.parquet /data/bronze/
    #     done
    #     """
    # )

    # # Use this instead if you don't want to use the get above. Just pre-prepare a yellow_tripdata file and upload to hdfs normally.
    # # hdfs dfs -mkdir /data
    # # hdfs dfs -put yellow_tripdata_2025.parquet /data

    # #This is an example of memory allocation:
    # # --driver-memory 512M \
    # #     --executor-memory 512M \
    # #     --executor-cores 1 \
    # #     --conf spark.yarn.am.memory=512M \
    # #     --conf spark.dynamicAllocation.enabled=false \
    # #     --conf spark.executor.instances=1 \

    # file setup for version 1: no streaming data.
    # file_setup_old = BashOperator(
    #     task_id="file_setup",
    #     bash_command="""
    #         hdfs dfs -mkdir -p /data/
    #         hdfs dfs -put -f /home/n3cr0d3m0nncrdmn/data/yellow_tripdata_2025.parquet /data/
    #         hdfs dfs -put -f /home/n3cr0d3m0nncrdmn/data/taxi_zone_lookup.csv /data/
    #     """
    # )
    # Updated file_setup using MinIO instead of HDFS
    # ingestion_old = BashOperator(
    #     task_id="ingestion",
    #     bash_command="""
    #     /opt/spark/bin/spark-submit \
    #     --master yarn \
    #     --deploy-mode client \
    #     --conf spark.yarn.jars="local:/opt/spark/jars/*" \
    #     --driver-memory 512M \
    #     --executor-memory 512M \
    #     --executor-cores 1 \
    #     --conf spark.yarn.am.memory=512M \
    #     --conf spark.dynamicAllocation.enabled=false \
    #     --conf spark.executor.instances=1 \
    #     --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    #     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    #     --conf spark.sql.catalog.bronze_catalog=org.apache.iceberg.spark.SparkCatalog \
    #     --conf spark.sql.catalog.bronze_catalog.type=hadoop \
    #     --conf spark.sql.catalog.bronze_catalog.warehouse=s3a://bronze/ \
    #     --conf spark.hadoop.fs.s3a.endpoint=http://10.140.0.4:9001 \
    #     --conf spark.hadoop.fs.s3a.access.key=admin \
    #     --conf spark.hadoop.fs.s3a.secret.key=12345678 \
    #     --conf spark.hadoop.fs.s3a.path.style.access=true \
    #     --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    #     --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    #     /home/n3cr0d3m0nncrdmn/ingestion.py
    # """
    # )
    pass

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

    # 1. Setup: Upload Local Data to MinIO Raw

    # 2. Bronze to Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/n3cr0d3m0nncrdmn/bronze_to_silver.py"
    )

    # 3. Silver to Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/n3cr0d3m0nncrdmn/silver_to_gold.py"
    )

    # 4. Validation/Reading tasks
    read_bronze = BashOperator(
        task_id="read_bronze",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/n3cr0d3m0nncrdmn/read_bronze.py"
    )

    learning_gold = BashOperator(
        task_id="learning_gold",
        bash_command="/opt/spark/bin/spark-submit --master yarn --deploy-mode client /home/n3cr0d3m0nncrdmn/learning_gold.py"
    )

    ingestion >> bronze_to_silver >> silver_to_gold >> read_bronze >> learning_gold
   #download_data >> create_bronze_dir >> upload_to_bronze >> bronze >> silver >> gold