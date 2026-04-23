from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
spark = SparkSession.builder \
    .appName("SilverLayer") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Classic minIO
# df = spark.read.parquet("s3a://bronze/yellow_tripdata_2025.parquet")

# Read from Iceberg
df = spark.read.parquet("s3a://bronze/")


print("### Reading bronze data, cleaning")
#df.printSchema()
#df.limit(20).show()
#print("original: ",df.count())

df = df.dropna('all')

# df.limit(20).show()
# print("dropped NULLs: ",df.count())

df = df.dropDuplicates()



#df.limit(20).show()
# print("dropped all duplicates: ",df.count())

# df = df.fillna('None',subset=['originating_base_num'])


# filtering bad values
df = df.filter(
    (col("fare_amount") >= 0) &
    (col("tip_amount") >= 0) &
    (col("total_amount") >= 0) &
    (col("trip_distance") >= 0) &
    (col("tolls_amount") >= 0) &
    (col("extra") >= 0) &
    (col("mta_tax") >= 0) &
    (col("improvement_surcharge") >= 0)
)
#df.sort(col('tolls').asc()).show() 


df = df.withColumn(
    "trip_duration_minutes",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
)

df = df.filter(col("trip_duration_minutes") > 0)

print("### Writing to silver")



# classic minIO
# df.write.mode("overwrite").parquet("s3a://silver/yellow_tripdata_2025.parquet")


# Write the cleaned data
df.writeTo("silver_catalog.default.yellow_taxi").createOrReplace()