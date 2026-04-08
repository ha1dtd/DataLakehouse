from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.148.0.9:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read from MinIO RAW
df = spark.read.parquet("s3a://raw/yellow_tripdata_2025.parquet")

print("### Bronze: Collected raw data")

# Preview BEFORE writing
print("### Bronze: Preview")
df.printSchema()
df.limit(15).show()

# Write with safe fallback
print("### Bronze: Writing to Iceberg")

try:
    df.writeTo("bronze_catalog.default.yellow_taxi").append()
    print("### Appended to existing table")
except Exception as e:
    print("### Table not found, creating new one")
    df.writeTo("bronze_catalog.default.yellow_taxi").createOrReplace()