import argparse
import math
import os
import tempfile

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, NumericType

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

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


def infer_numeric_features(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]


def to_numeric_df(df, cols):
    return df.select(*[F.col(c).cast(DoubleType()).alias(c) for c in cols])


def shuffled_sample_df(df, fraction, seed):
    return df.orderBy(F.rand(seed)).sample(withReplacement=False, fraction=fraction, seed=seed)


def build_histogram_map(df, cols, bins):
    if not cols:
        return {}

    stats = df.select(*cols).summary("min", "max").toPandas().set_index("summary")
    bucket_cols, meta = [], []

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

    binned = df.select(*[F.col(c) for c in cols], *bucket_cols)
    result = {}
    for c, cmin, cmax, width in meta:
        bin_col = f"__bin__{c}"
        counts = (binned.filter(F.col(c).isNotNull())
                  .groupBy(bin_col)
                  .count()
                  .orderBy(bin_col)
                  .collect())
        idx_to_count = {int(r[bin_col]): int(r["count"]) for r in counts if r[bin_col] is not None}
        xs = [cmin + (i + 0.5) * width for i in range(bins)]
        ys = [idx_to_count.get(i, 0) for i in range(bins)]
        result[c] = (xs, ys, width)
    return result


def plot_all_features(agg_map, out_file, title):
    cols = list(agg_map.keys())
    if not cols:
        return

    ncols = 4
    nrows = math.ceil(len(cols) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4 * nrows))
    axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

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
    parser.add_argument("--sample-fraction", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bins", type=int, default=50)
    args = parser.parse_args()

    spark = build_spark()
    plt.rc('font', size=12)
    plt.rc('axes', labelsize=12, titlesize=12)
    plt.rc('legend', fontsize=10)
    plt.rc('xtick', labelsize=9)
    plt.rc('ytick', labelsize=9)

    tmpdir = tempfile.mkdtemp(prefix="histograms_")

    for dataset, table in TABLES.items():
        df = spark.read.table(table)
        numeric_cols = infer_numeric_features(df)
        if not numeric_cols:
            print(f"skip {dataset}: no numeric columns")
            continue

        working_df = shuffled_sample_df(to_numeric_df(df, numeric_cols), args.sample_fraction, args.seed)
        if args.mode == "sample":
            working_df = working_df.limit(100000)

        agg_map = build_histogram_map(working_df, numeric_cols, args.bins)
        if not agg_map:
            print(f"skip {dataset}: no usable numeric features after sampling")
            continue

        out_file = os.path.join(tmpdir, f"{dataset}_{args.mode}_histogram.png")
        title = f"{dataset} - shuffled {int(args.sample_fraction * 100)}% histogram profile ({args.mode})"
        plot_all_features(agg_map, out_file, title)

        upload_with_aws(out_file, args.bucket, os.path.basename(out_file))
        print(f"uploaded: s3://{args.bucket}/{os.path.basename(out_file)}")

        if args.mode == "sample":
            print(f"\n--- {dataset} sampled describe ---")
            working_df.describe().show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
