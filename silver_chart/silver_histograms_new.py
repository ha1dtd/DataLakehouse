import argparse
import json
import math
import os
import subprocess
import tempfile

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, ScalarFormatter, FuncFormatter
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, NumericType

DATASET_DISPLAY_NAMES = {
    "yellow_taxi": "Yellow Taxi Trips",
    "green_taxi": "Green Taxi Trips",
    "fhv_trip": "For-Hired Vehicle (FHV) Trips",
    "fhvhv_trip": "High Volume For-Hired Vehicle (FHVHV) Trips",
}

FEATURE_DISPLAY_NAMES = {
    "trip_distance": "Trip Distance",
    "fare_amount": "Fare Amount",
    "total_amount": "Total Amount",
    "trip_duration_minutes": "Trip Duration",
    "mta_tax": "MTA Tax",
    "tolls_amount": "Toll Charges",
    "trip_miles": "Trip Miles",
    "trip_time_seconds": "Trip Time",
    "base_passenger_fare": "Base Passenger Fare",
    "bcf": "Black Car Fund Surcharge",
    "sales_tax": "Sales Tax",
    "driver_pay": "Driver Pay",
    "tolls": "Tolls",
    "tips": "Tips",
}

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"

TABLES = {
    "yellow_taxi": "silver_catalog.default.yellow_taxi",
    "green_taxi": "silver_catalog.default.green_taxi",
    "fhv_trip": "silver_catalog.default.fhv_trip",
    "fhvhv_trip": "silver_catalog.default.fhvhv_trip",
}

GLOBAL_IN_RANGE_HI_CAP = 200.0
DEFAULT_COVERAGE_QUANTILE = 0.995

IN_RANGE_HI_CAPS = {
    "tolls_amount": 200.0,
    "mta_tax": 20.0,
    "tips": 200.0,
    "tip_amount": 200.0,
}

PEAK_FEATURES_BY_DATASET = {
    "yellow_taxi": {
        "trip_distance", "fare_amount", "total_amount", "trip_duration_minutes", "mta_tax", "tolls_amount"
    },
    "green_taxi": {
        "trip_distance", "fare_amount", "total_amount", "trip_duration_minutes", "mta_tax", "tolls_amount"
    },
    "fhv_trip": {
        "trip_duration_minutes"
    },
    "fhvhv_trip": {
        "trip_miles", "trip_time_seconds", "base_passenger_fare", "bcf", "sales_tax", "driver_pay", "trip_duration_minutes", "tolls", "tips"
    },
}

STATIC_NUMERIC_FEATURES_BY_DATASET = {
    "yellow_taxi": sorted(list(PEAK_FEATURES_BY_DATASET["yellow_taxi"])),
    "green_taxi": sorted(list(PEAK_FEATURES_BY_DATASET["green_taxi"])),
    "fhv_trip": sorted(list(PEAK_FEATURES_BY_DATASET["fhv_trip"])),
    "fhvhv_trip": sorted(list(PEAK_FEATURES_BY_DATASET["fhvhv_trip"])),
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


def auto_outlier_bounds(df, cols, rel_err=0.02, bound_mode="two_stage_iqr", coverage_quantile=DEFAULT_COVERAGE_QUANTILE):
    bounds = {}
    for c in cols:
        try:
            q01, q25, _q50, q75, q99 = df.approxQuantile(c, [0.01, 0.25, 0.50, 0.75, 0.99], rel_err)
            if any(v is None or math.isnan(v) for v in [q01, q25, q75, q99]):
                continue

            iqr = q75 - q25
            if iqr <= 0:
                lo, hi = 0, q99
            else:
                lo = 0
                hi = min(q99, q75 + 3.0 * iqr)

            if bound_mode == "coverage_quantile":
                cq = max(0.90, min(float(coverage_quantile), 0.9999))
                q_cov = df.approxQuantile(c, [cq], rel_err)[0]
                if q_cov is not None and not math.isnan(q_cov):
                    hi = max(hi, float(q_cov))
            elif bound_mode == "two_stage_iqr":
                tail_df = df.filter(F.col(c).isNotNull() & (F.col(c) > F.lit(hi)))
                tqs = tail_df.approxQuantile(c, [0.25, 0.75, 0.99], rel_err)
                if len(tqs) == 3:
                    tq25, tq75, tq99 = tqs
                    if not any(v is None or math.isnan(v) for v in [tq25, tq75, tq99]):
                        tiqr = tq75 - tq25
                        if tiqr > 0:
                            hi2 = min(tq99, tq75 + 3.0 * tiqr)
                        else:
                            hi2 = tq99
                        if hi2 is not None and not math.isnan(hi2):
                            hi = max(hi, float(hi2))
            else:
                target_hi = IN_RANGE_HI_CAPS.get(c, GLOBAL_IN_RANGE_HI_CAP)
                hi = max(hi, float(target_hi))

            if hi <= lo:
                hi = lo + 1.0
            bounds[c] = (float(lo), float(hi))
        except Exception:
            continue
    return bounds


def compute_data_bounds(df, cols, min_floor_by_col=None, max_cap_by_col=None):
    min_floor_by_col = min_floor_by_col or {}
    max_cap_by_col = max_cap_by_col or {}
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

        cap = max_cap_by_col.get(c)
        if cap is not None and cmax > cap:
            cmax = cap

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

        width = max(1, round((cmax - cmin) / bins))
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
        xs = [int(round(cmin + (i + 0.5) * width)) for i in range(bins)]
        ys = [idx_to_count.get(i, 0) for i in range(bins)]
        result[c] = (xs, ys, width)
    return result


EXPLICIT_UNITS = {
    "trip_duration": "minutes",
    "trip_duration_minutes": "minutes",
    "duration_minutes": "minutes",
    "trip_time_seconds": "seconds",
    "trip_distance": "miles",
    "trip_miles": "miles",
    "distance_miles": "miles",
    "fare_amount": "USD",
    "tip_amount": "USD",
    "tips": "USD",
    "tolls": "USD",
    "tolls_amount": "USD",
    "extra": "USD",
    "mta_tax": "USD",
    "improvement_surcharge": "USD",
    "total_amount": "USD",
    "base_passenger_fare": "USD",
    "bcf": "USD",
    "sales_tax": "USD",
    "driver_pay": "USD",
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


def humanize_dataset_name(name):
    if name in DATASET_DISPLAY_NAMES:
        return DATASET_DISPLAY_NAMES[name]
    cleaned = name.replace("_", " ").strip()
    return " ".join(w.capitalize() for w in cleaned.split())

def humanize_feature_name(name):
    if name in FEATURE_DISPLAY_NAMES:
        return FEATURE_DISPLAY_NAMES[name]
    cleaned = name.replace("_", " ").strip()
    return " ".join(w.capitalize() for w in cleaned.split())

def format_unit(unit):
    mapping = {
        "USD": "USD ($)",
        "minutes": "minutes",
        "miles": "miles",
        "seconds": "seconds",
        "degrees": "degrees",
        "count": "count",
        "value": "value",
    }
    return mapping.get(unit, unit)

def safe_feature_folder_name(feature_name):
    return feature_name.replace("/", "_").replace(" ", "_")


def format_thousands_dot(v, _pos=None):
    try:
        return f"{int(round(v)):,}".replace(",", ".")
    except Exception:
        return str(v)

def plot_single_feature(xs, ys, width, out_file, title, feature_name, y_log=False, x_log=False, prefix=None, style="bar", raw_min=None, raw_max=None):
    unit = infer_unit(feature_name)
    feature_label = humanize_feature_name(feature_name)

    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(1, 1, figsize=(14.0, 9.0), facecolor="#f6f9ff")
    ax.set_facecolor("#eef4ff")

    is_peak = prefix == "PEAK"
    draw_xs = list(xs)
    draw_ys = list(ys)

    if is_peak:
        peak_pts = [(x, y) for x, y in zip(draw_xs, draw_ys) if x is not None and y is not None and x > 0]
        if peak_pts:
            draw_xs, draw_ys = zip(*peak_pts)
            draw_xs, draw_ys = list(draw_xs), list(draw_ys)

    if style == "line":
        pts = [(x, y) for x, y in zip(draw_xs, draw_ys) if x is not None and y is not None and y > 0]
        if x_log:
            pts = [(x, y) for x, y in pts if x > 0]

        if pts:
            px, py = zip(*pts)
            ax.plot(px, py, linewidth=2.5, color="#2563eb", label="Peak pattern")
            ax.fill_between(px, py, alpha=0.2, color="#60a5fa")
            ax.legend(loc="upper right", frameon=True)
        else:
            ax.text(0.5, 0.5, "No usable values for peak chart", ha="center", va="center", transform=ax.transAxes)
    else:
        ax.bar(draw_xs, draw_ys, width=width * 0.95, color="#3b82f6", edgecolor="#1e3a8a", linewidth=0.4)

    section_name = "Peak Distribution" if is_peak else None
    chart_title = f"{feature_label} - {section_name}" if section_name else feature_label
    ax.set_title(chart_title, fontsize=22, fontweight="bold", color="#0f172a")
    ax.set_xlabel(f"{feature_label} ({format_unit(unit)})", fontsize=16)
    ax.set_ylabel("Number of records", fontsize=16)
    ax.tick_params(axis="both", labelsize=13)
    ax.yaxis.set_major_formatter(FuncFormatter(format_thousands_dot))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=20, min_n_ticks=14))

    if is_peak and draw_xs:
        xmax = max(draw_xs)
        axis_start = 0
        tick_start = axis_start
        tick_end = int(math.ceil(xmax / 20.0) * 20)
        if tick_end <= tick_start:
            tick_end = tick_start + 20
        ticks = list(range(tick_start, tick_end + 1, 20))
        if ticks:
            ax.set_xticks(ticks)
        ax.set_xlim(left=axis_start)
    else:
        if draw_xs:
            step = max(1, len(draw_xs) // 12)
            tick_positions = [0] + draw_xs[::step]
            tick_positions = list(dict.fromkeys(tick_positions))
            ax.set_xticks(tick_positions)
            ax.set_xticklabels([f"{int(v)}" for v in tick_positions], rotation=0, ha="center")
            ax.set_xlim(left=0, right=max(draw_xs) + width)
        else:
            ax.xaxis.set_major_locator(MaxNLocator(8, integer=True))
            ax.set_xlim(left=0.0)

    if x_log:
        ax.set_xscale("log")
        sf = ScalarFormatter()
        sf.set_scientific(False)
        sf.set_useOffset(False)
        ax.xaxis.set_major_formatter(sf)

    if y_log:
        ax.set_yscale("log")

    if not y_log:
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _pos: "" if abs(y) < 1e-9 else format_thousands_dot(y)))

    ax.grid(axis="y", alpha=0.3, linestyle="--")
    if raw_min is not None and raw_max is not None:
        fig.text(0.5, 0.015, f"min: {raw_min:.2f}   max: {raw_max:.2f}", ha="center", va="bottom", fontsize=9, color="#0f172a")
    plt.tight_layout(rect=[0.006, 0.02, 0.995, 0.985])
    plt.savefig(out_file, dpi=150)
    plt.close(fig)


def plot_all_features(agg_map, out_file, title, y_log=False):
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
        if y_log:
            ax.set_yscale("log")

    for ax in axes[len(cols):]:
        ax.axis("off")

    fig.suptitle(title)
    plt.tight_layout()
    plt.savefig(out_file, dpi=150)
    plt.close(fig)


def plot_combined_inrange_peak(in_range_map, peak_map, out_file, title):
    in_cols = list(in_range_map.keys())
    peak_cols = list(peak_map.keys())
    max_cols = max(len(in_cols), len(peak_cols), 1)
    ncols = 4
    nrows = math.ceil(max_cols / ncols)

    fig, axes = plt.subplots(nrows * 2, ncols, figsize=(6 * ncols, 4 * nrows * 2))
    axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

    top_axes = axes[: nrows * ncols]
    bot_axes = axes[nrows * ncols :]

    for ax, c in zip(top_axes, in_cols):
        xs, ys, width = in_range_map[c]
        unit = infer_unit(c)
        ax.bar(xs, ys, width=width * 0.95)
        ax.set_title(f"IN_RANGE: {c}")
        ax.set_xlabel(f"{c} ({unit})")
        ax.set_ylabel("record_count")

    for ax in top_axes[len(in_cols):]:
        ax.axis("off")

    for ax, c in zip(bot_axes, peak_cols):
        xs, ys, width = peak_map[c]
        unit = infer_unit(c)
        ax.bar(xs, ys, width=width * 0.95)
        ax.set_title(f"PEAK: {c}")
        ax.set_xlabel(f"{c} ({unit})")
        ax.set_ylabel("record_count")
        ax.set_yscale("log")

    for ax in bot_axes[len(peak_cols):]:
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


def s3_object_exists(bucket, key):
    cmd = (
        f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
        f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
        f"aws --endpoint-url {MINIO_ENDPOINT} s3api head-object "
        f"--bucket '{bucket}' --key '{key}' > /dev/null 2>&1"
    )
    rc = os.system(cmd)
    return rc == 0

def list_existing_keys(bucket, prefix):
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
    env["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY

    cmd = [
        "aws", "--endpoint-url", MINIO_ENDPOINT,
        "s3api", "list-objects-v2",
        "--bucket", bucket,
        "--prefix", prefix,
        "--output", "json",
    ]

    try:
        out = subprocess.check_output(cmd, env=env, text=True)
        payload = json.loads(out) if out.strip() else {}
        contents = payload.get("Contents", []) or []
        return {item["Key"] for item in contents if isinstance(item, dict) and "Key" in item}
    except Exception:
        return set()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["sample", "full"], required=True)
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--sample-fraction", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--bins", type=int, default=50)
    parser.add_argument("--quantile-rel-err", type=float, default=0.02)
    parser.add_argument("--bound-mode", choices=["hard_cap", "two_stage_iqr", "coverage_quantile"], default="two_stage_iqr")
    parser.add_argument("--coverage-quantile", type=float, default=DEFAULT_COVERAGE_QUANTILE)
    parser.add_argument("--day", default="latest")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    tmpdir = tempfile.mkdtemp(prefix="histograms_")
    spark = build_spark()

    for dataset, table in TABLES.items():
        allowed_peak_features = PEAK_FEATURES_BY_DATASET.get(dataset, set())

        numeric_cols = STATIC_NUMERIC_FEATURES_BY_DATASET.get(dataset, [])
        if not numeric_cols:
            print(f"skip {dataset}: no static numeric features configured")
            continue
        numeric_cols_set = set(numeric_cols)

        df = spark.read.table(table)

        missing_inrange_features = set()
        missing_peak_features = set()
        skipped_count = 0

        existing_keys = set()
        if not args.overwrite:
            dataset_prefix = f"{args.day}/{dataset}/"
            existing_keys = list_existing_keys(args.bucket, dataset_prefix)

        for feature_name in numeric_cols:
            feature_folder = safe_feature_folder_name(feature_name)
            inrange_key = f"{args.day}/{dataset}/{feature_folder}/inrange.png"
            if args.overwrite or (inrange_key not in existing_keys):
                missing_inrange_features.add(feature_name)
            else:
                skipped_count += 1

        for feature_name in allowed_peak_features:
            if feature_name not in numeric_cols_set:
                continue
            feature_folder = safe_feature_folder_name(feature_name)
            peak_key = f"{args.day}/{dataset}/{feature_folder}/peak.png"
            if args.overwrite or (peak_key not in existing_keys):
                missing_peak_features.add(feature_name)
            else:
                skipped_count += 1

        missing_peak_features = {f for f in missing_peak_features if f in numeric_cols_set}

        needed_target_features = sorted(missing_inrange_features | missing_peak_features)
        if not needed_target_features:
            print(f"skip {dataset}: all required charts already exist (no Spark compute)")
            print(f"uploaded 0 chart image(s), skipped {skipped_count} existing image(s) for dataset={dataset} under s3://{args.bucket}/{args.day}/{dataset}/")
            continue

        working_df = sampled_df(
            to_numeric_df(df, numeric_cols),
            args.sample_fraction,
            args.seed,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        if args.mode == "sample":
            working_df = working_df.limit(100000).persist(StorageLevel.MEMORY_AND_DISK)

        # materialize once to avoid repeated lineage recompute across quantiles/splits/histograms
        _ = working_df.count()

        bounds = auto_outlier_bounds(
            working_df,
            needed_target_features,
            rel_err=args.quantile_rel_err,
            bound_mode=args.bound_mode,
            coverage_quantile=args.coverage_quantile,
        )
        if not bounds:
            print(f"skip {dataset}: no usable numeric features after outlier profiling")
            working_df.unpersist()
            continue

        needed_features = list(bounds.keys())
        if not needed_features:
            print(f"skip {dataset}: no bounded features after profiling")
            working_df.unpersist()
            continue

        in_range_df, peak_df = split_range_and_peak(working_df, bounds)

        in_range_map = build_histogram_map(in_range_df, needed_features, args.bins, bounds)
        if not in_range_map and not missing_peak_features:
            print(f"skip {dataset}: no usable numeric features after in-range filtering")
            working_df.unpersist()
            continue

        raw_extreme_bounds = compute_data_bounds(working_df, needed_features)
        peak_bounds = compute_data_bounds(
            peak_df,
            needed_features,
            min_floor_by_col={c: hi for c, (_lo, hi) in bounds.items() if c in needed_features},
        )
        peak_map = build_histogram_map(peak_df, list(peak_bounds.keys()), args.bins, peak_bounds)
        peak_map = {k: v for k, v in peak_map.items() if k in missing_peak_features}

        uploaded_count = 0
        for feature_name in needed_features:
            feature_folder = safe_feature_folder_name(feature_name)
            chart_dir = os.path.join(tmpdir, dataset, feature_folder)
            os.makedirs(chart_dir, exist_ok=True)

            if feature_name in missing_inrange_features and feature_name in in_range_map:
                xs, ys, width = in_range_map[feature_name]
                inrange_file = os.path.join(chart_dir, "inrange.png")
                inrange_key = f"{args.day}/{dataset}/{feature_folder}/inrange.png"
                inrange_title = f"{humanize_dataset_name(dataset)}"
                raw_min, raw_max = raw_extreme_bounds.get(feature_name, (None, None))
                plot_single_feature(
                    xs, ys, width, inrange_file, inrange_title, feature_name,
                    y_log=False, x_log=False, prefix="IN_RANGE", raw_min=raw_min, raw_max=raw_max
                )
                upload_with_aws(inrange_file, args.bucket, inrange_key)
                uploaded_count += 1

            if feature_name in peak_map:
                pxs, pys, pwidth = peak_map[feature_name]
                peak_file = os.path.join(chart_dir, "peak.png")
                peak_key = f"{args.day}/{dataset}/{feature_folder}/peak.png"
                peak_title = f"{humanize_dataset_name(dataset)}"
                raw_min, raw_max = raw_extreme_bounds.get(feature_name, (None, None))
                plot_single_feature(
                    pxs, pys, pwidth, peak_file, peak_title, feature_name,
                    y_log=False, x_log=True, prefix="PEAK", style="line", raw_min=raw_min, raw_max=raw_max
                )
                upload_with_aws(peak_file, args.bucket, peak_key)
                uploaded_count += 1

        print(f"uploaded {uploaded_count} chart image(s), skipped {skipped_count} existing image(s) for dataset={dataset} under s3://{args.bucket}/{args.day}/{dataset}/")

        if args.mode == "sample":
            print(f"\n--- {dataset} sampled describe ---")
            working_df.describe().show(truncate=False)

        working_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
