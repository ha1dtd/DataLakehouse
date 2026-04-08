from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "you",
    "start_date": datetime(2026, 3, 23),
    "retries": 1
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.hadoop.fs.s3a.endpoint=http://10.148.0.6:9001 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=12345678 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        /home/haidtd2003/bronze_to_silver.py
        """
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        spark-submit \
        --master yarn \
        --deploy-mode client \
        --conf spark.hadoop.fs.s3a.endpoint=http://10.148.0.6:9001 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=12345678 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        /home/haidtd2003/silver_to_gold.py
        """
    )

    bronze_to_silver >> silver_to_gold