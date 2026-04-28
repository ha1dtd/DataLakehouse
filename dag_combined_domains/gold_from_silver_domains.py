import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, dayofweek, hour, round, sum, when

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://192.168.100.66:9001")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "12345678")
DOMAIN_REGISTRY_FILE = os.environ.get("DOMAIN_REGISTRY_FILE", "/home/ubuntu/daihai_script/dag_combined_domains/domain_registry_v2.json")

spark = SparkSession.builder \
    .appName("GoldFromSilverDomains") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

with open(DOMAIN_REGISTRY_FILE, "r", encoding="utf-8") as f:
    domain_registry = json.load(f)

domains = domain_registry.get("domains", {})


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def create_namespace_if_needed(catalog: str, namespace: str):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def process_taxi_domain(domain_name: str, domain_cfg: dict):
    silver_namespace = domain_cfg.get("silver_namespace", domain_name)
    gold_namespace = domain_cfg.get("gold_namespace", domain_name)

    create_namespace_if_needed("gold_catalog", gold_namespace)

    if table_exists(f"silver_catalog.{silver_namespace}.yellow_taxi"):
        yellow_df = spark.read.table(f"silver_catalog.{silver_namespace}.yellow_taxi")
        yellow_base = yellow_df.withColumn("pickup_hour", hour("pickup_datetime")) \
            .withColumn("pickup_weekday", dayofweek("pickup_datetime"))
        yellow_base.cache()
        yellow_base.count()

        yellow_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            "tip_amount",
            "fare_amount",
            round(when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0), 2).alias("tip_ratio"),
            (col("tip_amount") > 0).alias("is_tipped")
        ).writeTo(f"gold_catalog.{gold_namespace}.yellow_taxi_tips").createOrReplace()

        yellow_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            "trip_distance",
            "trip_duration_minutes",
            round(when(col("trip_duration_minutes") > 0, col("trip_distance") / (col("trip_duration_minutes") / 60.0)).otherwise(0), 2).alias("avg_speed_mph"),
            round(when(col("trip_duration_minutes") > 0, col("total_amount") / col("trip_duration_minutes")).otherwise(0), 2).alias("revenue_per_minute")
        ).writeTo(f"gold_catalog.{gold_namespace}.yellow_taxi_performance").createOrReplace()

        yellow_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            round((col("surcharge") + col("mta_tax") + col("tolls_amount")), 2).alias("total_fees"),
            "total_amount",
            round(when(col("trip_distance") > 0, col("total_amount") / col("trip_distance")).otherwise(0), 2).alias("cost_per_mile"),
            round(when(col("passenger_count") > 0, col("total_amount") / col("passenger_count")).otherwise(0), 2).alias("fare_per_passenger")
        ).writeTo(f"gold_catalog.{gold_namespace}.yellow_taxi_financials").createOrReplace()

        yellow_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            (col("pickup_hour").between(7, 9) | col("pickup_hour").between(16, 19)).alias("is_peak_hour"),
            col("pickup_weekday").isin([1, 7]).alias("is_weekend"),
            when(col("trip_distance") < 2, "short").when(col("trip_distance") < 10, "medium").otherwise("long").alias("trip_type"),
            col("payment_type").isin("CRD", "CRE", "1").alias("is_card_payment"),
            ((col("trip_distance") <= 0) | (col("fare_amount") <= 0) | (col("trip_duration_minutes") <= 0)).alias("is_suspicious")
        ).writeTo(f"gold_catalog.{gold_namespace}.yellow_taxi_classifications").createOrReplace()

        yellow_base.unpersist()

    if table_exists(f"silver_catalog.{silver_namespace}.green_taxi"):
        green_df = spark.read.table(f"silver_catalog.{silver_namespace}.green_taxi")
        green_base = green_df.withColumn("pickup_hour", hour("pickup_datetime")) \
            .withColumn("pickup_weekday", dayofweek("pickup_datetime"))
        green_base.cache()
        green_base.count()

        green_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            "tip_amount",
            "fare_amount",
            round(when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount")).otherwise(0), 2).alias("tip_ratio"),
            (col("tip_amount") > 0).alias("is_tipped")
        ).writeTo(f"gold_catalog.{gold_namespace}.green_taxi_tips").createOrReplace()

        green_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            "trip_distance",
            "trip_duration_minutes",
            round(when(col("trip_duration_minutes") > 0, col("trip_distance") / (col("trip_duration_minutes") / 60.0)).otherwise(0), 2).alias("avg_speed_mph"),
            round(when(col("trip_duration_minutes") > 0, col("total_amount") / col("trip_duration_minutes")).otherwise(0), 2).alias("revenue_per_minute")
        ).writeTo(f"gold_catalog.{gold_namespace}.green_taxi_performance").createOrReplace()

        green_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            round((col("extra") + col("mta_tax") + col("tolls_amount") + col("improvement_surcharge")), 2).alias("total_fees"),
            "total_amount",
            round(when(col("trip_distance") > 0, col("total_amount") / col("trip_distance")).otherwise(0), 2).alias("cost_per_mile"),
            round(when(col("passenger_count") > 0, col("total_amount") / col("passenger_count")).otherwise(0), 2).alias("fare_per_passenger")
        ).writeTo(f"gold_catalog.{gold_namespace}.green_taxi_financials").createOrReplace()

        green_base.select(
            "domain",
            "vendor_id",
            "pickup_datetime",
            (col("pickup_hour").between(7, 9) | col("pickup_hour").between(16, 19)).alias("is_peak_hour"),
            col("pickup_weekday").isin([1, 7]).alias("is_weekend"),
            when(col("trip_distance") < 2, "short").when(col("trip_distance") < 10, "medium").otherwise("long").alias("trip_type"),
            col("payment_type").isin("1").alias("is_card_payment"),
            ((col("trip_distance") <= 0) | (col("fare_amount") <= 0) | (col("trip_duration_minutes") <= 0)).alias("is_suspicious")
        ).writeTo(f"gold_catalog.{gold_namespace}.green_taxi_classifications").createOrReplace()

        green_base.unpersist()

    if table_exists(f"silver_catalog.{silver_namespace}.fhv_trip"):
        spark.read.table(f"silver_catalog.{silver_namespace}.fhv_trip").groupBy("domain", "dispatching_base_num").agg(
            count("*").alias("trip_count"),
            round(sum("trip_duration_minutes"), 2).alias("total_trip_minutes"),
            round(sum(when(col("sr_flag").isNotNull(), 1).otherwise(0)), 2).alias("sr_flag_trip_count")
        ).writeTo(f"gold_catalog.{gold_namespace}.fhv_trip_summary").createOrReplace()

    if table_exists(f"silver_catalog.{silver_namespace}.fhvhv_trip"):
        spark.read.table(f"silver_catalog.{silver_namespace}.fhvhv_trip").groupBy("domain", "hvfhs_license_num").agg(
            count("*").alias("trip_count"),
            round(sum("trip_miles"), 2).alias("total_trip_miles"),
            round(sum("base_passenger_fare"), 2).alias("total_base_passenger_fare"),
            round(sum("tips"), 2).alias("total_tips"),
            round(sum("driver_pay"), 2).alias("total_driver_pay")
        ).writeTo(f"gold_catalog.{gold_namespace}.fhvhv_trip_summary").createOrReplace()


def process_non_taxi_domain(domain_name: str, domain_cfg: dict):
    silver_namespace = domain_cfg.get("silver_namespace", domain_name)
    gold_namespace = domain_cfg.get("gold_namespace", domain_name)
    topics = domain_cfg.get("topics", {})

    create_namespace_if_needed("gold_catalog", gold_namespace)

    for topic_name, topic_cfg in topics.items():
        silver_table = topic_cfg.get("silver_table", f"{topic_name}_silver")
        gold_table = topic_cfg.get("gold_table", f"{topic_name}_summary")

        silver_fqn = f"silver_catalog.{silver_namespace}.{silver_table}"
        gold_fqn = f"gold_catalog.{gold_namespace}.{gold_table}"

        if not table_exists(silver_fqn):
            continue

        df = spark.read.table(silver_fqn)

        if domain_name == "hr":
            out = df.groupBy("domain", "department", "attendance_status").agg(
                count("*").alias("record_count"),
                round(sum("hours_worked"), 2).alias("total_hours_worked")
            )
        elif domain_name == "finance":
            out = df.groupBy("domain", "cost_center").agg(
                count("*").alias("record_count"),
                round(sum("money_input"), 2).alias("total_money_input"),
                round(sum("money_output"), 2).alias("total_money_output"),
                round(sum("profit"), 2).alias("total_profit")
            )
        elif domain_name == "marketing":
            out = df.groupBy("domain", "channel").agg(
                count("*").alias("record_count"),
                round(sum("spend_usd"), 2).alias("total_spend_usd"),
                sum("clicks").alias("total_clicks"),
                sum("conversions").alias("total_conversions")
            ).withColumn(
                "conversion_rate",
                round(when(col("total_clicks") > 0, col("total_conversions") / col("total_clicks")).otherwise(0.0), 4)
            )
        else:
            out = df.groupBy("domain").agg(count("*").alias("record_count"))

        out.writeTo(gold_fqn).createOrReplace()
        print(f"### Wrote {gold_fqn}")

for domain_name, domain_cfg in domains.items():
    if not domain_cfg.get("enabled", True):
        continue
    if "taxi" in domain_cfg:
        process_taxi_domain(domain_name, domain_cfg)
    else:
        process_non_taxi_domain(domain_name, domain_cfg)

print("### Gold domain pipeline completed")
