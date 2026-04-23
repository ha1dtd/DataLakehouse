from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp

spark = SparkSession.builder \
    .appName("GoldFromIngestedStructured") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.100.66:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

if spark.catalog.tableExists("silver_catalog.default.ingested_structured_data"):
    df = spark.read.table("silver_catalog.default.ingested_structured_data")
    summary = df.groupBy("source_name", "file_type").agg(count("*").alias("record_count")) \
        .withColumn("gold_processed_ts", current_timestamp())
    summary.writeTo("gold_catalog.default.ingested_structured_summary").createOrReplace()
    print("GOLD_WRITTEN=1")
else:
    print("GOLD_WRITTEN=0")
