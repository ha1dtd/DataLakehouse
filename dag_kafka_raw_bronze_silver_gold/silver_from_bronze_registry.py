from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

spark = SparkSession.builder \
    .appName("SilverFromBronzeRegistry") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.100.66:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS silver_catalog.default")

registry = spark.read.table("bronze_catalog.default.raw_file_registry").filter(col("status") == "success")

frames = []
for row in registry.collect():
    bronze_uri = f"s3a://bronze/{row['bronze_object_key']}"
    df = spark.read.parquet(bronze_uri)
    df = df.withColumn("job_id", lit(row["job_id"])) \
           .withColumn("source_name", lit(row["source_name"])) \
           .withColumn("file_type", lit(row["file_type"])) \
           .withColumn("bronze_object_key", lit(row["bronze_object_key"])) \
           .withColumn("silver_processed_ts", current_timestamp())
    frames.append(df)

if frames:
    silver_df = frames[0]
    for df in frames[1:]:
        silver_df = silver_df.unionByName(df, allowMissingColumns=True)
    silver_df.writeTo("silver_catalog.default.ingested_structured_data").createOrReplace()
    print("SILVER_WRITTEN=1")
else:
    print("SILVER_WRITTEN=0")
