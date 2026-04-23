from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, pandas_udf, log as spark_log
import pandas as pd
import matplotlib.pyplot as plt
import math
import os

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"
OUTPUT_BUCKET = "histogram"
OUTPUT_PREFIX = "taxi_feature_histograms"
# Fraction of records to sample per dataset. 0.01 = 1%
SAMPLE_FRACTION = float(os.environ.get("HIST_SAMPLE_FRACTION", "0.01"))
# Cap the absolute number of rows sampled per feature to keep histogram compute sane
MAX_SAMPLE_ROWS = int(os.environ.get("HIST_MAX_SAMPLE_ROWS", "500000"))

spark = SparkSession.builder \
    .appName("SilverHistogramsSample") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

sc = spark.sparkContext
jvm = sc._jvm
hadoop_conf = sc._jsc.hadoopConfiguration()

# ---------------------------------------------------------------------------
# Per-dataset feature lists (numeric only, manually curated for clarity)
# ---------------------------------------------------------------------------
FEATURES = {
    "yellow_taxi": [
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "tolls_amount",
        "mta_tax",
        "surcharge",
    ],
    "green_taxi": [
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "tolls_amount",
        "mta_tax",
        "extra",
        "improvement_surcharge",
    ],
    "fhv_trip": [
        "pu_location_id",
        "do_location_id",
        "trip_duration_minutes",
    ],
    "fhvhv_trip": [
        "pu_location_id",
        "do_location_id",
        "trip_miles",
        "trip_duration_minutes",
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
    ],
}

# Iceberg table names
TABLES = [
    "silver_catalog.default.yellow_taxi",
    "silver_catalog.default.green_taxi",
    "silver_catalog.default.fhv_trip",
    "silver_catalog.default.fhvhv_trip",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def hdfs_mkdirs(path: str):
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    fs.mkdirs(jvm.org.apache.hadoop.fs.Path(path))


def upload_file(local_path: str, dest_path: str):
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    fs.copyFromLocalFile(
        False, True,
        jvm.org.apache.hadoop.fs.Path(local_path),
        jvm.org.apache.hadoop.fs.Path(dest_path),
    )


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def is_numeric_col(col_name: str, dtype: str) -> bool:
    numeric_prefixes = {"int", "bigint", "float", "double", "decimal", "smallint", "tinyint", "long", "short"}
    return any(dtype.startswith(t) for t in numeric_prefixes)


def pick_plotable_features(df, feature_list):
    dtype_map = dict(df.dtypes)
    return [f for f in feature_list if dtype_map.get(f) and is_numeric_col(f, dtype_map[f])]


def sample_for_histogram(df, feature_name):
    """
    Shuffle-step: add a random column, filter to SAMPLE_FRACTION, then cap at MAX_SAMPLE_ROWS.
    This breaks any natural clustering in the source data (e.g. files sorted by year,
    or one dataset dominating the partition layout) so the sample is truly representative.
    """
    sampled = (
        df.withColumn("_rn", rand())
          .where(col("_rn") < SAMPLE_FRACTION)
          .select(col(feature_name).cast("double").alias(feature_name))
          .where(col(feature_name).isNotNull())
          .limit(MAX_SAMPLE_ROWS)
    )
    return sampled.toPandas()[feature_name].dropna()


def plot_features(series_by_name, dataset_name):
    n = len(series_by_name)
    cols = 3
    rows = math.ceil(n / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(cols * 6, rows * 4))
    axes_flat = axes.flatten() if hasattr(axes, "flatten") else [axes]

    for idx, (feature, series) in enumerate(series_by_name.items()):
        ax = axes_flat[idx]
        ax.hist(series, bins=40, color="#4e79a7", edgecolor="black", alpha=0.85)
        ax.set_title(feature, fontsize=12)
        ax.set_xlabel(feature)
        ax.set_ylabel("count")

    # Hide unused subplots
    for idx in range(n, len(axes_flat)):
        fig.delaxes(axes_flat[idx])

    fig.suptitle(f"{dataset_name} — feature distributions (sample={int(SAMPLE_FRACTION*100)}%)", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    local_path = f"/tmp/{dataset_name}_histograms.png"
    fig.savefig(local_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return local_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
hdfs_mkdirs(f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/sample/")

for table_name in TABLES:
    dataset_name = table_name.split(".")[-1]
    features = FEATURES.get(dataset_name, [])

    if not table_exists(table_name):
        print(f"### Skip {table_name}: not found")
        continue

    df = spark.read.table(table_name)
    plottable = pick_plotable_features(df, features)

    if not plottable:
        print(f"### Skip {dataset_name}: no plottable numeric features")
        continue

    print(f"### {dataset_name}: sampling {SAMPLE_FRACTION*100:.1f}% of rows for {len(plottable)} features")
    series_by_name = {}
    for feature in plottable:
        try:
            series = sample_for_histogram(df, feature)
            if not series.empty:
                series_by_name[feature] = series
        except Exception as e:
            print(f"  [WARN] Skipped '{feature}': {e}")

    if not series_by_name:
        print(f"### Skip {dataset_name}: no data collected")
        continue

    local_path = plot_features(series_by_name, dataset_name)
    dest_path = f"s3a://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/sample/{dataset_name}_histograms.png"
    upload_file(local_path, dest_path)
    print(f"### Uploaded → {dest_path}")

print("### Done.")
print(df.describe)
spark.stop()
