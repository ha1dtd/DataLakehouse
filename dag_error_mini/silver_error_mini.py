import functools
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, to_timestamp, unix_timestamp

spark = SparkSession.builder \
    .appName("SilverLayerErrorMini") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.100.66:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "12345678") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

sc = spark.sparkContext
jvm = sc._jvm
hadoop_conf = sc._jsc.hadoopConfiguration()

print("### Bronze to Silver (mini): normalize schema families safely")


def glob_paths(pattern: str):
    path_obj = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path_obj.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(path_obj)
    if not statuses:
        return []
    return sorted([status.getPath().toString() for status in statuses])


def first_paths(pattern: str, limit: int = 10):
    paths = glob_paths(pattern)
    return paths[:limit]


def detect_yellow_schema(path: str) -> str:
    raw = spark.read.parquet(path).limit(1)
    cols = set(raw.columns)
    if "Trip_Pickup_DateTime" in cols:
        return "legacy"
    elif "tpep_pickup_datetime" in cols:
        return "modern"
    else:
        return "intermediate"


def read_and_merge_varying_schemas(spark, paths):
    from pyspark.sql.functions import col
    from pyspark.sql.types import NumericType, DoubleType, StringType

    dfs = [spark.read.parquet(path) for path in paths]

    unified_types = {}
    for df in dfs:
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType

            if col_name not in unified_types:
                unified_types[col_name] = col_type
            elif type(unified_types[col_name]) != type(col_type):
                if isinstance(unified_types[col_name], NumericType) and isinstance(col_type, NumericType):
                    unified_types[col_name] = DoubleType()
                else:
                    unified_types[col_name] = StringType()

    aligned_dfs = []
    for df in dfs:
        for col_name in df.columns:
            target_type = unified_types[col_name]
            if type(df.schema[col_name].dataType) != type(target_type):
                df = df.withColumn(col_name, col(col_name).cast(target_type))
        aligned_dfs.append(df)

    return functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), aligned_dfs)


# ----------------------
# YELLOW
# ----------------------
yellow_paths = first_paths("s3a://bronze/raw/yellow_tripdata_*.parquet", 10)

if yellow_paths:
    groups = {"legacy": [], "modern": [], "intermediate": []}
    for path in yellow_paths:
        schema_type = detect_yellow_schema(path)
        groups[schema_type].append(path)

    branch_dfs = []

    if groups["legacy"]:
        raw = read_and_merge_varying_schemas(spark, groups["legacy"])
        df = raw.select(
            col("vendor_name").cast("string").alias("vendor_id"),
            to_timestamp(col("Trip_Pickup_DateTime")).alias("pickup_datetime"),
            to_timestamp(col("Trip_Dropoff_DateTime")).alias("dropoff_datetime"),
            col("Passenger_Count").cast("int").alias("passenger_count"),
            col("Trip_Distance").cast("double").alias("trip_distance"),
            col("Payment_Type").cast("string").alias("payment_type"),
            col("Fare_Amt").cast("double").alias("fare_amount"),
            col("Tip_Amt").cast("double").alias("tip_amount"),
            col("Tolls_Amt").cast("double").alias("tolls_amount"),
            col("Total_Amt").cast("double").alias("total_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            coalesce(col("surcharge"), lit(0.0)).cast("double").alias("surcharge"),
            col("Rate_Code").cast("string").alias("rate_code"),
            col("store_and_forward").cast("string").alias("store_and_fwd_flag"),
            col("Start_Lon").cast("double").alias("pickup_longitude"),
            col("Start_Lat").cast("double").alias("pickup_latitude"),
            col("End_Lon").cast("double").alias("dropoff_longitude"),
            col("End_Lat").cast("double").alias("dropoff_latitude")
        )
        branch_dfs.append(df)
        print(f"  legacy yellow files: {len(groups['legacy'])}")

    if groups["modern"]:
        raw = read_and_merge_varying_schemas(spark, groups["modern"])
        df = raw.select(
            col("VendorID").cast("string").alias("vendor_id"),
            col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("payment_type").cast("string").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("total_amount").cast("double").alias("total_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            coalesce(col("extra"), lit(0.0)).cast("double").alias("surcharge"),
            col("RatecodeID").cast("string").alias("rate_code"),
            col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
            lit(None).cast("double").alias("pickup_longitude"),
            lit(None).cast("double").alias("pickup_latitude"),
            lit(None).cast("double").alias("dropoff_longitude"),
            lit(None).cast("double").alias("dropoff_latitude")
        )
        branch_dfs.append(df)
        print(f"  modern yellow files: {len(groups['modern'])}")

    if groups["intermediate"]:
        raw = read_and_merge_varying_schemas(spark, groups["intermediate"])
        df = raw.select(
            col("vendor_id").cast("string").alias("vendor_id"),
            to_timestamp(col("pickup_datetime")).alias("pickup_datetime"),
            to_timestamp(col("dropoff_datetime")).alias("dropoff_datetime"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("payment_type").cast("string").alias("payment_type"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount"),
            col("total_amount").cast("double").alias("total_amount"),
            col("mta_tax").cast("double").alias("mta_tax"),
            coalesce(col("surcharge"), lit(0.0)).cast("double").alias("surcharge"),
            col("rate_code").cast("string").alias("rate_code"),
            col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
            col("pickup_longitude").cast("double").alias("pickup_longitude"),
            col("pickup_latitude").cast("double").alias("pickup_latitude"),
            col("dropoff_longitude").cast("double").alias("dropoff_longitude"),
            col("dropoff_latitude").cast("double").alias("dropoff_latitude")
        )
        branch_dfs.append(df)
        print(f"  intermediate yellow files: {len(groups['intermediate'])}")

    if branch_dfs:
        yellow_silver = functools.reduce(lambda df1, df2: df1.unionByName(df2), branch_dfs)

        yellow_silver = yellow_silver.filter(
            col("pickup_datetime").isNotNull() &
            col("dropoff_datetime").isNotNull() &
            col("trip_distance").isNotNull() &
            col("fare_amount").isNotNull() &
            col("tip_amount").isNotNull() &
            col("total_amount").isNotNull() &
            (col("fare_amount") >= 0) &
            (col("tip_amount") >= 0) &
            (col("total_amount") >= 0) &
            (col("trip_distance") >= 0) &
            (col("tolls_amount") >= 0)
        ).withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
        ).filter(col("trip_duration_minutes") > 0)

        yellow_silver.writeTo("silver_catalog.default.yellow_taxi_error_mini").createOrReplace()
        print("### Wrote silver_catalog.default.yellow_taxi_error_mini")
else:
    print("### Skip yellow_taxi: no matching sampled raw files")

# ----------------------
# GREEN
# ----------------------
green_paths = first_paths("s3a://bronze/raw/green_tripdata_*.parquet", 10)

if green_paths:
    green_raw = read_and_merge_varying_schemas(spark, green_paths)
    green_silver = green_raw.select(
        col("VendorID").cast("string").alias("vendor_id"),
        col("lpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("lpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("passenger_count").cast("int").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("payment_type").cast("string").alias("payment_type"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("tolls_amount").cast("double").alias("tolls_amount"),
        col("total_amount").cast("double").alias("total_amount"),
        col("mta_tax").cast("double").alias("mta_tax"),
        coalesce(col("extra"), lit(0.0)).cast("double").alias("extra"),
        coalesce(col("improvement_surcharge"), lit(0.0)).cast("double").alias("improvement_surcharge"),
        col("RatecodeID").cast("string").alias("rate_code"),
        col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        col("PULocationID").cast("int").alias("pu_location_id"),
        col("DOLocationID").cast("int").alias("do_location_id"),
        col("trip_type").cast("double").alias("trip_type_raw")
    )

    green_silver = green_silver.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("trip_distance").isNotNull() &
        col("fare_amount").isNotNull() &
        col("tip_amount").isNotNull() &
        col("total_amount").isNotNull() &
        (col("fare_amount") >= 0) &
        (col("tip_amount") >= 0) &
        (col("total_amount") >= 0) &
        (col("trip_distance") >= 0) &
        (col("tolls_amount") >= 0)
    ).withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    ).filter(col("trip_duration_minutes") > 0)

    green_silver.writeTo("silver_catalog.default.green_taxi_error_mini").createOrReplace()
    print("### Wrote silver_catalog.default.green_taxi_error_mini")
else:
    print("### Skip green_taxi: no matching sampled raw files")

# ----------------------
# FHV
# ----------------------
fhv_paths = first_paths("s3a://bronze/raw/fhv_tripdata_*.parquet", 10)

if fhv_paths:
    fhv_raw = read_and_merge_varying_schemas(spark, fhv_paths)
    fhv_silver = fhv_raw.select(
        col("dispatching_base_num").cast("string").alias("dispatching_base_num"),
        col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("dropOff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("PUlocationID").cast("int").alias("pu_location_id"),
        col("DOlocationID").cast("int").alias("do_location_id"),
        col("SR_Flag").cast("string").alias("sr_flag"),
        col("Affiliated_base_number").cast("string").alias("affiliated_base_number")
    )

    fhv_silver = fhv_silver.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull()
    ).withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    ).filter(col("trip_duration_minutes") > 0)

    fhv_silver.writeTo("silver_catalog.default.fhv_trip_error_mini").createOrReplace()
    print("### Wrote silver_catalog.default.fhv_trip_error_mini")
else:
    print("### Skip fhv_trip: no matching sampled raw files")

# ----------------------
# FHVHV
# ----------------------
fhvhv_paths = first_paths("s3a://bronze/raw/fhvhv_tripdata_*.parquet", 10)

if fhvhv_paths:
    fhvhv_raw = read_and_merge_varying_schemas(spark, fhvhv_paths)
    fhvhv_silver = fhvhv_raw.select(
        col("hvfhs_license_num").cast("string").alias("hvfhs_license_num"),
        col("dispatching_base_num").cast("string").alias("dispatching_base_num"),
        col("originating_base_num").cast("string").alias("originating_base_num"),
        col("request_datetime").cast("timestamp").alias("request_datetime"),
        col("on_scene_datetime").cast("timestamp").alias("on_scene_datetime"),
        col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        col("PULocationID").cast("int").alias("pu_location_id"),
        col("DOLocationID").cast("int").alias("do_location_id"),
        col("trip_miles").cast("double").alias("trip_miles"),
        col("trip_time").cast("bigint").alias("trip_time_seconds"),
        col("base_passenger_fare").cast("double").alias("base_passenger_fare"),
        col("tolls").cast("double").alias("tolls"),
        col("bcf").cast("double").alias("bcf"),
        col("sales_tax").cast("double").alias("sales_tax"),
        col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col("airport_fee").cast("double").alias("airport_fee"),
        col("tips").cast("double").alias("tips"),
        col("driver_pay").cast("double").alias("driver_pay"),
        col("shared_request_flag").cast("string").alias("shared_request_flag"),
        col("shared_match_flag").cast("string").alias("shared_match_flag"),
        col("access_a_ride_flag").cast("string").alias("access_a_ride_flag"),
        col("wav_request_flag").cast("string").alias("wav_request_flag"),
        col("wav_match_flag").cast("string").alias("wav_match_flag")
    )

    fhvhv_silver = fhvhv_silver.filter(
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        col("trip_miles").isNotNull() &
        col("base_passenger_fare").isNotNull() &
        (col("trip_miles") >= 0) &
        (col("base_passenger_fare") >= 0)
    ).withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    ).filter(col("trip_duration_minutes") > 0)

    fhvhv_silver.writeTo("silver_catalog.default.fhvhv_trip_error_mini").createOrReplace()
    print("### Wrote silver_catalog.default.fhvhv_trip_error_mini")
else:
    print("### Skip fhvhv_trip: no matching sampled raw files")
