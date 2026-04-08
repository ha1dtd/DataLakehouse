from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, avg

# Start Spark session (YARN will be used if configured)
spark = SparkSession.builder.appName("demo").getOrCreate()

# Read parquet (fix the path: remove the trailing dot)
df = spark.read.parquet(
    "hdfs://namenode:9000/user/haidtd2003/input/yellow_tripdata_2025-01.parquet"
)

# Preview
df.printSchema()
df.show(10)

# =========================
# ENRICHMENT
# =========================

# Extract pickup hour
df = df.withColumn(
    "pickup_hour",
    hour(col("tpep_pickup_datetime"))
)

# Create time bucket
df = df.withColumn(
    "time_bucket",
    when(col("pickup_hour").between(6, 11), "morning")
    .when(col("pickup_hour").between(12, 17), "afternoon")
    .otherwise("night")
)

# =========================
# ANALYSIS
# =========================

result = df.groupBy("time_bucket") \
    .agg(avg("total_amount").alias("avg_revenue")) \
    .orderBy("time_bucket")

result.show()

# =========================
# OUTPUT
# =========================

# 1. Distributed output (HDFS)
result.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/output/revenue_by_time"
)

# 2. Single file output (for demo)
result.coalesce(1).write.mode("overwrite").parquet(
    "hdfs://namenode:9000/output/revenue_by_time_single"
)

# Stop session
spark.stop()