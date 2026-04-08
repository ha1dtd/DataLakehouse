from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("silver_to_gold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.148.0.9:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3AFileSystem") \
    .getOrCreate()

print("=== READ FROM SILVER ===")
df = spark.read.parquet("s3a://silver/trip_enriched/")

print("=== AGGREGATION ===")
result = df.groupBy("time_bucket") \
    .agg(avg("total_amount").alias("avg_revenue")) \
    .orderBy("time_bucket")

print("=== PREVIEW GOLD ===")
result.show()

print("=== WRITE TO GOLD ===")
result.write.mode("overwrite").parquet("s3a://gold/revenue_by_time/")

spark.stop()