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


def sampled_df(df, fraction, seed):
    return df.sample(withReplacement=False, fraction=fraction, seed=seed)


def auto_outlier_bounds(df, cols, rel_err=0.01):
    bounds = {}
    for c in cols:
        try:
            q01, q25, _q50, q75, q99 = df.approxQuantile(c, [0.01, 0.25, 0.50, 0.75, 0.99], rel_err)
            if any(v is None or math.isnan(v) for v in [q01, q25, q75, q99]):
                continue

            iqr = q75 - q25
            if iqr <= 0:
                lo, hi = q01, q99
            else:
                lo = max(q01, q25 - 3.0 * iqr)
                hi = min(q99, q75 + 3.0 * iqr)

            if hi <= lo:
                hi = lo + 1.0
            bounds[c] = (float(lo), float(hi))
        except Exception:
            continue
    return bounds


def compute_data_bounds(df, cols, min_floor_by_col=None):
    min_floor_by_col = min_floor_by_col or {}
    bounds = {}
    if not cols:
        return bounds

    agg_exprs = []
    for c in cols:
        agg_exprs.append(F.min(F.col(c)).alias(f"__min__{c}"))
        agg_exprs.append(F.max(F.col(c)).alias(f"__max__{c}"))

    row = df.agg(*agg_exprs).collect()[0]
    for c in cols:
        cmin = row[f"__min__{c}"]
        cmax = row[f"__max__{c}"]
        if cmin is None or cmax is None:
            continue

        floor = min_floor_by_col.get(c)
        if floor is not None and cmin < floor:
            cmin = floor

        if cmax <= cmin:
            cmax = cmin + 1.0

        bounds[c] = (float(cmin), float(cmax))

    return bounds


def clip_to_bounds(df, bounds):
    clipped = df
    for c, (lo, hi) in bounds.items():
        clipped = clipped.withColumn(
            c,
            F.when(F.col(c).isNull(), None)
             .when(F.col(c) < F.lit(lo), F.lit(lo))
             .when(F.col(c) > F.lit(hi), F.lit(hi))
             .otherwise(F.col(c))
        )
    return clipped


def split_range_and_peak(df, bounds):
    in_range_cond = F.lit(True)
    peak_cond = F.lit(False)

    for c, (_lo, hi) in bounds.items():
        in_range_cond = in_range_cond & (F.col(c).isNull() | (F.col(c) <= F.lit(hi)))
        peak_cond = peak_cond | (F.col(c).isNotNull() & (F.col(c) > F.lit(hi)))

    in_range_df = df.filter(in_range_cond)
    peak_df = df.filter(peak_cond)
    return in_range_df, peak_df

def build_histogram_map(df, cols, bins, bounds):
    if not cols:
        return {}

    bucket_cols, meta = [], []

    for c in cols:
        if c not in bounds:
            continue
        cmin, cmax = bounds[c]
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


EXPLICIT_UNITS = {
    "trip_duration": "minutes",
    "trip_duration_minutes": "minutes",
    "duration_minutes": "minutes",
    "trip_time_seconds": "seconds",
    "trip_distance": "miles",
    "distance_miles": "miles",
    "fare_amount": "USD",
    "tip_amount": "USD",
    "tolls_amount": "USD",
    "extra": "USD",
    "mta_tax": "USD",
    "improvement_surcharge": "USD",
    "total_amount": "USD",
    "pickup_latitude": "degrees",
    "pickup_longitude": "degrees",
    "dropoff_latitude": "degrees",
    "dropoff_longitude": "degrees",
    "passenger_count": "count",
}


def infer_unit(col_name):
    key = col_name.lower()
    unit = EXPLICIT_UNITS.get(key)
    if unit:
        return unit

    if "duration" in key or key.endswith("_minutes"):
        return "minutes"
    if "distance" in key or "miles" in key:
        return "miles"
    if "amount" in key or "fare" in key or "tip" in key or "toll" in key or "tax" in key or "surcharge" in key or "pay" in key:
        return "USD"
    if "time_seconds" in key or key.endswith("_seconds"):
        return "seconds"
    if "count" in key:
        return "count"
    if "latitude" in key or "longitude" in key:
        return "degrees"
    return "value"


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
        unit = infer_unit(c)
        ax.bar(xs, ys, width=width * 0.95)
        ax.set_title(c)
        ax.set_xlabel(f"{c} ({unit})")
        ax.set_ylabel("record_count")

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

        working_df = sampled_df(
            to_numeric_df(df, numeric_cols),
            args.sample_fraction,
            args.seed,
        ).cache()

        if args.mode == "sample":
            working_df = working_df.limit(100000).cache()

        bounds = auto_outlier_bounds(working_df, numeric_cols)
        if not bounds:
            print(f"skip {dataset}: no usable numeric features after outlier profiling")
            continue

        in_range_df, peak_df = split_range_and_peak(working_df, bounds)

        in_range_map = build_histogram_map(in_range_df, list(bounds.keys()), args.bins, bounds)
        if not in_range_map:
            print(f"skip {dataset}: no usable numeric features after in-range filtering")
            continue

        in_range_file = os.path.join(tmpdir, f"{dataset}_{args.mode}_histogram_in_range.png")
        in_range_title = f"{dataset} - random {int(args.sample_fraction * 100)}% histogram profile ({args.mode}, peak-removed)"
        plot_all_features(in_range_map, in_range_file, in_range_title)
        upload_with_aws(in_range_file, args.bucket, os.path.basename(in_range_file))
        print(f"uploaded: s3://{args.bucket}/{os.path.basename(in_range_file)}")

        peak_bounds = compute_data_bounds(
            peak_df,
            list(bounds.keys()),
            min_floor_by_col={c: hi for c, (_lo, hi) in bounds.items()},
        )
        peak_map = build_histogram_map(peak_df, list(peak_bounds.keys()), args.bins, peak_bounds)
        if peak_map:
            peak_file = os.path.join(tmpdir, f"{dataset}_{args.mode}_histogram_peak.png")
            peak_title = f"{dataset} - random {int(args.sample_fraction * 100)}% histogram profile ({args.mode}, peak-only, starts-above-in-range)"
            plot_all_features(peak_map, peak_file, peak_title)
            upload_with_aws(peak_file, args.bucket, os.path.basename(peak_file))
            print(f"uploaded: s3://{args.bucket}/{os.path.basename(peak_file)}")
        else:
            print(f"info {dataset}: no high-end peak values found to render peak-only chart")

        if args.mode == "sample":
            print(f"\n--- {dataset} sampled describe ---")
            working_df.describe().show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
