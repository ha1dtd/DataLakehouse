import argparse
import math
import os
import tempfile
from io import BytesIO

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

FEATURES = {
    "yellow_taxi": [
        "passenger_count", "trip_distance", "trip_duration_minutes",
        "fare_amount", "tip_amount", "tolls_amount",
        "total_amount", "mta_tax", "surcharge",
    ],
    "green_taxi": [
        "passenger_count", "trip_distance", "trip_duration_minutes",
        "fare_amount", "tip_amount", "tolls_amount",
        "total_amount", "mta_tax", "extra",
    ],
    "fhv_trip": [
        "trip_duration_minutes", "pu_location_id", "do_location_id",
    ],
    "fhvhv_trip": [
        "trip_miles", "trip_duration_minutes", "base_passenger_fare",
        "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee", "tips",
    ],
}

TABLES = {
    "yellow_taxi": "silver_catalog.default.yellow_taxi",
    "green_taxi": "silver_catalog.default.green_taxi",
    "fhv_trip": "silver_catalog.default.fhv_trip",
    "fhvhv_trip": "silver_catalog.default.fhvhv_trip",
}


def build_spark():
    return SparkSession.builder \
        .appName("SilverHistogramGenerator") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def safe_features(df, dataset):
    cols = set(df.columns)
    return [c for c in FEATURES[dataset] if c in cols]


def sampled_pdf(df, cols, mode):
    base = df.select(*[F.col(c).cast("double").alias(c) for c in cols])
    for c in cols:
        base = base.filter(F.col(c).isNotNull())

    if mode == "sample":
        sampled = base.sample(withReplacement=False, fraction=0.02, seed=42).limit(100000)
        return sampled.toPandas()

    agg_exprs = []
    bins = 50
    stats = df.select(*cols).summary("min", "max").toPandas().set_index("summary")
    bucket_cols = []
    meta = []
    for c in cols:
        cmin = float(stats.loc["min", c])
        cmax = float(stats.loc["max", c])
        if math.isnan(cmin) or math.isnan(cmax):
            continue
        if cmax <= cmin:
            cmax = cmin + 1.0
        width = (cmax - cmin) / bins
        bucket = F.when(F.col(c).isNull(), None).otherwise(
            F.when(F.col(c) >= cmax, F.lit(bins - 1)).otherwise(
                F.floor((F.col(c) - F.lit(cmin)) / F.lit(width))
            )
        ).cast("int").alias(f"__bin__{c}")
        bucket_cols.append(bucket)
        meta.append((c, cmin, cmax, width))

    binned = df.select(*[F.col(c).cast("double").alias(c) for c in cols], *bucket_cols)

    result = {}
    for c, cmin, cmax, width in meta:
        bin_col = f"__bin__{c}"
        counts = (binned.filter(F.col(c).isNotNull())
                  .groupBy(bin_col)
                  .count()
                  .orderBy(bin_col)
                  .collect())
        xs = []
        ys = []
        idx_to_count = {int(r[bin_col]): int(r["count"]) for r in counts if r[bin_col] is not None}
        for i in range(bins):
            xs.append(cmin + (i + 0.5) * width)
            ys.append(idx_to_count.get(i, 0))
        result[c] = (xs, ys, width)
    return result


def plot_sample(pdf, cols, out_file, title):
    fig, axes = plt.subplots(3, 3, figsize=(18, 12))
    axes = axes.flatten()
    for ax, c in zip(axes, cols):
        s = pdf[c].dropna()
        ax.hist(s, bins=50)
        ax.set_title(c)
    for ax in axes[len(cols):]:
        ax.axis("off")
    fig.suptitle(title)
    plt.tight_layout()
    plt.savefig(out_file, dpi=150)
    plt.close(fig)


def plot_full(agg_map, cols, out_file, title):
    fig, axes = plt.subplots(3, 3, figsize=(18, 12))
    axes = axes.flatten()
    for ax, c in zip(axes, cols):
        xs, ys, width = agg_map[c]
        ax.bar(xs, ys, width=width * 0.95)
        ax.set_title(c)
    for ax in axes[len(cols):]:
        ax.axis("off")
    fig.suptitle(title)
    plt.tight_layout()
    plt.savefig(out_file, dpi=150)
    plt.close(fig)


def upload_with_aws(local_file, bucket, key):
    cmd = (
        f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
        f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
        f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp '{local_file}' s3://{bucket}/{key}"
    )
    rc = os.system(cmd)
    if rc != 0:
        raise RuntimeError(f"upload failed: {cmd}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sample", "full"], required=True)
    parser.add_argument("--bucket", default="histogram")
    args = parser.parse_args()

    spark = build_spark()
    plt.rc('font', size=12)
    plt.rc('axes', labelsize=12, titlesize=12)
    plt.rc('legend', fontsize=10)
    plt.rc('xtick', labelsize=9)
    plt.rc('ytick', labelsize=9)

    tmpdir = tempfile.mkdtemp(prefix="histograms_")

    # Store sampled dataframes for describe later (sample mode only)
    sample_dfs = {}

    for dataset, table in TABLES.items():
        df = spark.read.table(table)
        cols = list(dict.fromkeys(safe_features(df, dataset)))[:9]
        out_file = os.path.join(tmpdir, f"{dataset}_{args.mode}_histogram.png")
        title = f"{dataset} - {args.mode} histogram profile"
        
        if args.mode == "sample":
            # Keep the sampled df for describe later
            sampled = df.select(*[F.col(c).cast("double").alias(c) for c in cols])
            for c in cols:
                sampled = sampled.filter(F.col(c).isNotNull())
            sampled = sampled.sample(withReplacement=False, fraction=0.02, seed=42).limit(100000)
            sample_dfs[dataset] = (sampled, cols)
            pdf = sampled.toPandas()
            plot_sample(pdf, cols, out_file, title)
        else:
            agg_map = sampled_pdf(df, cols, args.mode)
            plot_full(agg_map, cols, out_file, title)
        
        upload_with_aws(out_file, args.bucket, os.path.basename(out_file))
        print(f"uploaded: s3://{args.bucket}/{os.path.basename(out_file)}")

    # After all datasets processed, show describe on sampled data (sample mode only)
    if args.mode == "sample" and sample_dfs:
        print("\n=== SAMPLE DESCRIBE ===")
        for dataset, (sampled_df, cols) in sample_dfs.items():
            print(f"\n--- {dataset} (sampled) ---")
            sampled_df.describe().show()

    spark.stop()


if __name__ == "__main__":
    main()