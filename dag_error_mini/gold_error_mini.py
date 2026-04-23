from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, dayofweek, hour, round, sum, when

spark = SparkSession.builder \
    .appName("GoldLayerErrorMini") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.100.66:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("### Reading silver mini data, creating simple gold mini tables")


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


# ----------------------
# YELLOW GOLD
# ----------------------
if table_exists("silver_catalog.default.yellow_taxi_error_mini"):
    yellow_df = spark.read.table("silver_catalog.default.yellow_taxi_error_mini")
    yellow_base = yellow_df.withColumn("pickup_hour", hour("pickup_datetime")) \
        .withColumn("pickup_weekday", dayofweek("pickup_datetime"))

    yellow_base.cache()
    yellow_base.count()

    yellow_tip_df = yellow_base.select(
        "vendor_id",
        "pickup_datetime",
        "tip_amount",
        "fare_amount",
        round(when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0), 2).alias("tip_ratio"),
        (col("tip_amount") > 0).alias("is_tipped")
    )
    yellow_tip_df.writeTo("gold_catalog.default.yellow_taxi_tips_error_mini").createOrReplace()

    yellow_perf_df = yellow_base.select(
        "vendor_id",
        "pickup_datetime",
        "trip_distance",
        "trip_duration_minutes",
        round(when(col("trip_duration_minutes") > 0, col("trip_distance") / (col("trip_duration_minutes") / 60.0)).otherwise(0), 2).alias("avg_speed_mph"),
        round(when(col("trip_duration_minutes") > 0, col("total_amount") / col("trip_duration_minutes")).otherwise(0), 2).alias("revenue_per_minute")
    )
    yellow_perf_df.writeTo("gold_catalog.default.yellow_taxi_performance_error_mini").createOrReplace()

    yellow_fin_df = yellow_base.select(
        "vendor_id",
        "pickup_datetime",
        round((col("surcharge") + col("mta_tax") + col("tolls_amount")), 2).alias("total_fees"),
        "total_amount",
        round(when(col("trip_distance") > 0, col("total_amount") / col("trip_distance")).otherwise(0), 2).alias("cost_per_mile"),
        round(when(col("passenger_count") > 0, col("total_amount") / col("passenger_count")).otherwise(0), 2).alias("fare_per_passenger")
    )
    yellow_fin_df.writeTo("gold_catalog.default.yellow_taxi_financials_error_mini").createOrReplace()

    yellow_class_df = yellow_base.select(
        "vendor_id",
        "pickup_datetime",
        (col("pickup_hour").between(7, 9) | col("pickup_hour").between(16, 19)).alias("is_peak_hour"),
        col("pickup_weekday").isin([1, 7]).alias("is_weekend"),
        when(col("trip_distance") < 2, "short").when(col("trip_distance") < 10, "medium").otherwise("long").alias("trip_type"),
        col("payment_type").isin("CRD", "CRE", "1").alias("is_card_payment"),
        ((col("trip_distance") <= 0) | (col("fare_amount") <= 0) | (col("trip_duration_minutes") <= 0)).alias("is_suspicious")
    )
    yellow_class_df.writeTo("gold_catalog.default.yellow_taxi_classifications_error_mini").createOrReplace()

    yellow_base.unpersist()
else:
    print("### Skip yellow gold mini: silver mini table missing")

# ----------------------
# GREEN GOLD
# ----------------------
if table_exists("silver_catalog.default.green_taxi_error_mini"):
    green_df = spark.read.table("silver_catalog.default.green_taxi_error_mini")
    green_base = green_df.withColumn("pickup_hour", hour("pickup_datetime")) \
        .withColumn("pickup_weekday", dayofweek("pickup_datetime"))

    green_base.cache()
    green_base.count()

    green_tip_df = green_base.select(
        "vendor_id",
        "pickup_datetime",
        "tip_amount",
        "fare_amount",
        round(when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0), 2).alias("tip_ratio"),
        (col("tip_amount") > 0).alias("is_tipped")
    )
    green_tip_df.writeTo("gold_catalog.default.green_taxi_tips_error_mini").createOrReplace()

    green_perf_df = green_base.select(
        "vendor_id",
        "pickup_datetime",
        "trip_distance",
        "trip_duration_minutes",
        round(when(col("trip_duration_minutes") > 0, col("trip_distance") / (col("trip_duration_minutes") / 60.0)).otherwise(0), 2).alias("avg_speed_mph"),
        round(when(col("trip_duration_minutes") > 0, col("total_amount") / col("trip_duration_minutes")).otherwise(0), 2).alias("revenue_per_minute")
    )
    green_perf_df.writeTo("gold_catalog.default.green_taxi_performance_error_mini").createOrReplace()

    green_fin_df = green_base.select(
        "vendor_id",
        "pickup_datetime",
        round((col("extra") + col("mta_tax") + col("tolls_amount") + col("improvement_surcharge")), 2).alias("total_fees"),
        "total_amount",
        round(when(col("trip_distance") > 0, col("total_amount") / col("trip_distance")).otherwise(0), 2).alias("cost_per_mile"),
        round(when(col("passenger_count") > 0, col("total_amount") / col("passenger_count")).otherwise(0), 2).alias("fare_per_passenger")
    )
    green_fin_df.writeTo("gold_catalog.default.green_taxi_financials_error_mini").createOrReplace()

    green_class_df = green_base.select(
        "vendor_id",
        "pickup_datetime",
        (col("pickup_hour").between(7, 9) | col("pickup_hour").between(16, 19)).alias("is_peak_hour"),
        col("pickup_weekday").isin([1, 7]).alias("is_weekend"),
        when(col("trip_distance") < 2, "short").when(col("trip_distance") < 10, "medium").otherwise("long").alias("trip_type"),
        col("payment_type").isin("1").alias("is_card_payment"),
        ((col("trip_distance") <= 0) | (col("fare_amount") <= 0) | (col("trip_duration_minutes") <= 0)).alias("is_suspicious")
    )
    green_class_df.writeTo("gold_catalog.default.green_taxi_classifications_error_mini").createOrReplace()

    green_base.unpersist()
else:
    print("### Skip green gold mini: silver mini table missing")

# ----------------------
# FHV GOLD
# ----------------------
if table_exists("silver_catalog.default.fhv_trip_error_mini"):
    fhv_df = spark.read.table("silver_catalog.default.fhv_trip_error_mini")
    fhv_summary = fhv_df.groupBy("dispatching_base_num").agg(
        count("*").alias("trip_count"),
        round(sum("trip_duration_minutes"), 2).alias("total_trip_minutes"),
        round(sum(when(col("sr_flag").isNotNull(), 1).otherwise(0)), 2).alias("sr_flag_trip_count")
    )
    fhv_summary.writeTo("gold_catalog.default.fhv_trip_summary_error_mini").createOrReplace()
else:
    print("### Skip fhv gold mini: silver mini table missing")

# ----------------------
# FHVHV GOLD
# ----------------------
if table_exists("silver_catalog.default.fhvhv_trip_error_mini"):
    fhvhv_df = spark.read.table("silver_catalog.default.fhvhv_trip_error_mini")
    fhvhv_summary = fhvhv_df.groupBy("hvfhs_license_num").agg(
        count("*").alias("trip_count"),
        round(sum("trip_miles"), 2).alias("total_trip_miles"),
        round(sum("base_passenger_fare"), 2).alias("total_base_passenger_fare"),
        round(sum("tips"), 2).alias("total_tips"),
        round(sum("driver_pay"), 2).alias("total_driver_pay")
    )
    fhvhv_summary.writeTo("gold_catalog.default.fhvhv_trip_summary_error_mini").createOrReplace()
else:
    print("### Skip fhvhv gold mini: silver mini table missing")

print("### Gold Layer Error Mini Tables Created Successfully")
