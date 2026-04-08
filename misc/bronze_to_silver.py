from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when

spark = SparkSession.builder \
    .appName("bronze_to_silver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.148.0.9:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3AFileSystem") \
    .getOrCreate()

print("=== READ FROM BRONZE ===")
df = spark.read.parquet("s3a://bronze/")

print("=== CLEANING ===")
df = df.dropna(subset=["tpep_pickup_datetime", "total_amount"])
df = df.filter(col("total_amount") > 0)

print("=== ENRICHMENT ===")
df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

df = df.withColumn(
    "time_bucket",
    when(col("pickup_hour").between(6, 11), "morning")
    .when(col("pickup_hour").between(12, 17), "afternoon")
    .otherwise("night")
)

print("=== PREVIEW SILVER ===")
df.printSchema()
df.show(10, truncate=False)

print("=== WRITE TO SILVER ===")
df.write.mode("overwrite").parquet("s3a://silver/trip_enriched/")

spark.stop()