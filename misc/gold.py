from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# classic minIO
# df_parquet = spark.read.parquet("s3a://silver/yellow_tripdata_2025.parquet")


# Read from Iceberg
df = spark.read.table("silver_catalog.default.yellow_taxi")
print("### Reading silver data, aggregating")
#df_parquet.printSchema()
#df_parquet.limit(15).show()


# trip duration:
df = df.withColumn(
    "trip_duration_minutes",
    expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)")
)


df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
       .withColumn("pickup_weekday", dayofweek("tpep_pickup_datetime"))


# total fees:
df = df.withColumn(
    "total_fees",
    col("extra") +
    col("mta_tax") +
    col("tolls_amount") +
    col("improvement_surcharge") +
    col("congestion_surcharge") +
    col("Airport_fee") +
    col("cbd_congestion_fee")
)
# tip ratio:
df = df.withColumn(
    "tip_ratio",
    when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount"))
    .otherwise(0)
)

# cost per data:
df = df.withColumn(
    "cost_per_mile",
    when(col("trip_distance") > 0, col("total_amount") / col("trip_distance")).otherwise(0)
)
# speed:
df = df.withColumn(
    "avg_speed_mph",
    when(col("trip_duration_minutes") > 0,
         col("trip_distance") / (col("trip_duration_minutes") / 60)
    ).otherwise(0)
)
# peak hour flag
df = df.withColumn(
    "is_peak_hour",
    col("pickup_hour").between(7, 9) | col("pickup_hour").between(16, 19)
)
# weekend or weekday
df = df.withColumn(
    "is_weekend",
    col("pickup_weekday").isin([1, 7])  # Sunday=1, Saturday=7
)
# trip type
df = df.withColumn(
    "trip_type",
    when(col("trip_distance") < 2, "short")
    .when(col("trip_distance") < 10, "medium")
    .otherwise("long")
)
# is credit card
df = df.withColumn(
    "is_card_payment",
    col("payment_type") == 1  # 1 = credit card in NYC taxi data
)
# tipped flag
df = df.withColumn(
    "tipped",
    col("tip_amount") > 0
)
# suspicious trips
df = df.withColumn(
    "is_suspicious",
    (col("trip_distance") <= 0) |
    (col("fare_amount") <= 0) |
    (col("trip_duration_minutes") <= 0)
)
# extreme tip ratio
df = df.withColumn(
    "high_tip",
    col("tip_ratio") > 1  # >100% tip
)
# revenue per minute
df = df.withColumn(
    "revenue_per_minute",
    when(col("trip_duration_minutes") > 0,
         col("total_amount") / col("trip_duration_minutes")
    ).otherwise(0)
)
# passenger efficiency
df = df.withColumn(
    "fare_per_passenger",
    when(col("passenger_count") > 0,
         col("total_amount") / col("passenger_count")
    ).otherwise(0)
)

# rounding data:
df = df.withColumn("tip_ratio", round(col("tip_ratio"), 2))
df = df.withColumn("total_fees", round(col("total_fees"), 2))
df = df.withColumn("cost_per_mile", round(col("cost_per_mile"), 2))
df = df.withColumn("avg_speed_mph", round(col("avg_speed_mph"), 2))
df = df.withColumn("fare_per_passenger", round(col("fare_per_passenger"), 2))
df = df.withColumn("revenue_per_minute", round(col("revenue_per_minute"), 2))

df.show()

print("### Writing to gold")

# classic minIO
# df.write.mode("overwrite").parquet("s3a://gold/yellow_tripdata_2025.parquet")

# Write the cleaned data
df.writeTo("gold_catalog.default.yellow_taxi").createOrReplace()