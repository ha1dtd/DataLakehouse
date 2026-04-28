import functools
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_timestamp, lit, to_timestamp, unix_timestamp

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://192.168.100.66:9001")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "12345678")
DOMAIN_REGISTRY_FILE = os.environ.get("DOMAIN_REGISTRY_FILE", "/home/ubuntu/daihai_script/dag_combined_domains/domain_registry_v2.json")

spark = SparkSession.builder \
    .appName("SilverFromBronzeDomains") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

with open(DOMAIN_REGISTRY_FILE, "r", encoding="utf-8") as f:
    domain_registry = json.load(f)

domains = domain_registry.get("domains", {})

sc = spark.sparkContext
jvm = sc._jvm
hadoop_conf = sc._jsc.hadoopConfiguration()


def glob_paths(pattern: str):
    path_obj = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path_obj.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(path_obj)
    if not statuses:
        return []
    return sorted([status.getPath().toString() for status in statuses])


def detect_yellow_schema(path: str) -> str:
    raw = spark.read.parquet(path).limit(1)
    cols = set(raw.columns)
    if "Trip_Pickup_DateTime" in cols:
        return "legacy"
    elif "tpep_pickup_datetime" in cols:
        return "modern"
    else:
        return "intermediate"


def read_and_merge_varying_schemas(paths):
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


def create_namespace_if_needed(catalog: str, namespace: str):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def process_taxi_domain(domain_name: str, domain_cfg: dict):
    bronze_bucket = domain_cfg.get("bronze_bucket", "bronze")
    bronze_prefix = domain_cfg.get("bronze_prefix", f"lakehouse/domains/{domain_name}/bronze")
    silver_namespace = domain_cfg.get("silver_namespace", domain_name)
    datasets = domain_cfg.get("taxi", {}).get("datasets", {})

    create_namespace_if_needed("silver_catalog", silver_namespace)

    yellow_paths = glob_paths(f"s3a://{bronze_bucket}/{bronze_prefix}/yellow/*/*.parquet")
    if yellow_paths:
        groups = {"legacy": [], "modern": [], "intermediate": []}
        for path in yellow_paths:
            groups[detect_yellow_schema(path)].append(path)

        branch_dfs = []
        if groups["legacy"]:
            raw = read_and_merge_varying_schemas(groups["legacy"])
            branch_dfs.append(raw.select(
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
                col("End_Lat").cast("double").alias("dropoff_latitude"),
            ))
        if groups["modern"]:
            raw = read_and_merge_varying_schemas(groups["modern"])
            branch_dfs.append(raw.select(
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
                lit(None).cast("double").alias("dropoff_latitude"),
            ))
        if groups["intermediate"]:
            raw = read_and_merge_varying_schemas(groups["intermediate"])
            branch_dfs.append(raw.select(
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
                col("dropoff_latitude").cast("double").alias("dropoff_latitude"),
            ))

        if branch_dfs:
            yellow_silver = functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), branch_dfs)
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
            ).filter(col("trip_duration_minutes") > 0) \
             .withColumn("domain", lit(domain_name)) \
             .withColumn("dataset", lit("yellow")) \
             .withColumn("silver_processed_ts", current_timestamp())
            yellow_silver.writeTo(f"silver_catalog.{silver_namespace}.{datasets.get('yellow', {}).get('silver_table', 'yellow_taxi')}").createOrReplace()
            print(f"### Wrote silver_catalog.{silver_namespace}.yellow_taxi")

    green_paths = glob_paths(f"s3a://{bronze_bucket}/{bronze_prefix}/green/*/*.parquet")
    if green_paths:
        green_raw = read_and_merge_varying_schemas(green_paths)
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
        ).filter(
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
        ).filter(col("trip_duration_minutes") > 0) \
         .withColumn("domain", lit(domain_name)) \
         .withColumn("dataset", lit("green")) \
         .withColumn("silver_processed_ts", current_timestamp())
        green_silver.writeTo(f"silver_catalog.{silver_namespace}.{datasets.get('green', {}).get('silver_table', 'green_taxi')}").createOrReplace()
        print(f"### Wrote silver_catalog.{silver_namespace}.green_taxi")

    fhv_paths = glob_paths(f"s3a://{bronze_bucket}/{bronze_prefix}/fhv/*/*.parquet")
    if fhv_paths:
        fhv_raw = read_and_merge_varying_schemas(fhv_paths)
        fhv_silver = fhv_raw.select(
            col("dispatching_base_num").cast("string").alias("dispatching_base_num"),
            col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            col("dropOff_datetime").cast("timestamp").alias("dropoff_datetime"),
            col("PUlocationID").cast("int").alias("pu_location_id"),
            col("DOlocationID").cast("int").alias("do_location_id"),
            col("SR_Flag").cast("string").alias("sr_flag"),
            col("Affiliated_base_number").cast("string").alias("affiliated_base_number")
        ).filter(
            col("pickup_datetime").isNotNull() &
            col("dropoff_datetime").isNotNull()
        ).withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
        ).filter(col("trip_duration_minutes") > 0) \
         .withColumn("domain", lit(domain_name)) \
         .withColumn("dataset", lit("fhv")) \
         .withColumn("silver_processed_ts", current_timestamp())
        fhv_silver.writeTo(f"silver_catalog.{silver_namespace}.{datasets.get('fhv', {}).get('silver_table', 'fhv_trip')}").createOrReplace()
        print(f"### Wrote silver_catalog.{silver_namespace}.fhv_trip")

    fhvhv_paths = glob_paths(f"s3a://{bronze_bucket}/{bronze_prefix}/fhvhv/*/*.parquet")
    if fhvhv_paths:
        fhvhv_raw = read_and_merge_varying_schemas(fhvhv_paths)
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
        ).filter(
            col("pickup_datetime").isNotNull() &
            col("dropoff_datetime").isNotNull() &
            col("trip_miles").isNotNull() &
            col("base_passenger_fare").isNotNull() &
            (col("trip_miles") >= 0) &
            (col("base_passenger_fare") >= 0)
        ).withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
        ).filter(col("trip_duration_minutes") > 0) \
         .withColumn("domain", lit(domain_name)) \
         .withColumn("dataset", lit("fhvhv")) \
         .withColumn("silver_processed_ts", current_timestamp())
        fhvhv_silver.writeTo(f"silver_catalog.{silver_namespace}.{datasets.get('fhvhv', {}).get('silver_table', 'fhvhv_trip')}").createOrReplace()
        print(f"### Wrote silver_catalog.{silver_namespace}.fhvhv_trip")


def process_non_taxi_domain(domain_name: str, domain_cfg: dict):
    bronze_bucket = domain_cfg.get("bronze_bucket", "bronze")
    bronze_prefix = domain_cfg.get("bronze_prefix", f"lakehouse/domains/{domain_name}/bronze")
    silver_namespace = domain_cfg.get("silver_namespace", domain_name)
    topics = domain_cfg.get("topics", {})

    create_namespace_if_needed("silver_catalog", silver_namespace)

    for topic_name, topic_cfg in topics.items():
        silver_table = topic_cfg.get("silver_table", f"{topic_name}_silver")
        paths = glob_paths(f"s3a://{bronze_bucket}/{bronze_prefix}/{topic_name}/*/*.parquet")
        if not paths:
            continue

        raw = read_and_merge_varying_schemas(paths)

        if domain_name == "hr":
            silver_df = raw.select(
                col("employee_id").cast("string").alias("employee_id"),
                to_timestamp(col("day_worked")).alias("day_worked"),
                col("department").cast("string").alias("department"),
                col("hours_worked").cast("double").alias("hours_worked"),
                col("attendance_status").cast("string").alias("attendance_status"),
                col("topic").cast("string").alias("topic")
            ).filter(
                col("employee_id").isNotNull() &
                col("day_worked").isNotNull() &
                col("hours_worked").isNotNull() &
                (col("hours_worked") >= 0)
            )
        elif domain_name == "finance":
            silver_df = raw.select(
                col("finance_id").cast("string").alias("finance_id"),
                to_timestamp(col("report_date")).alias("report_date"),
                col("money_input").cast("double").alias("money_input"),
                col("money_output").cast("double").alias("money_output"),
                col("profit").cast("double").alias("profit"),
                col("cost_center").cast("string").alias("cost_center"),
                col("topic").cast("string").alias("topic")
            ).filter(
                col("finance_id").isNotNull() &
                col("report_date").isNotNull() &
                col("money_input").isNotNull() &
                col("money_output").isNotNull() &
                col("profit").isNotNull() &
                (col("money_input") >= 0) &
                (col("money_output") >= 0)
            )
        elif domain_name == "marketing":
            silver_df = raw.select(
                col("campaign_id").cast("string").alias("campaign_id"),
                col("lead_id").cast("string").alias("lead_id"),
                col("channel").cast("string").alias("channel"),
                col("spend_usd").cast("double").alias("spend_usd"),
                col("clicks").cast("long").alias("clicks"),
                col("conversions").cast("long").alias("conversions"),
                col("event_time").cast("timestamp").alias("event_time"),
                col("topic").cast("string").alias("topic")
            ).filter(
                col("campaign_id").isNotNull() &
                col("event_time").isNotNull() &
                col("spend_usd").isNotNull() &
                col("clicks").isNotNull() &
                col("conversions").isNotNull() &
                (col("spend_usd") >= 0) &
                (col("clicks") >= 0) &
                (col("conversions") >= 0)
            )
        else:
            silver_df = raw

        silver_df = silver_df.withColumn("domain", lit(domain_name)) \
                             .withColumn("dataset", lit(topic_name)) \
                             .withColumn("silver_processed_ts", current_timestamp())

        silver_df.writeTo(f"silver_catalog.{silver_namespace}.{silver_table}").createOrReplace()
        print(f"### Wrote silver_catalog.{silver_namespace}.{silver_table}")

for domain_name, domain_cfg in domains.items():
    if not domain_cfg.get("enabled", True):
        continue
    if "taxi" in domain_cfg:
        process_taxi_domain(domain_name, domain_cfg)
    else:
        process_non_taxi_domain(domain_name, domain_cfg)
