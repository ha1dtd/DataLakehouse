import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"


def aws_cp_from_s3(bucket, key, local_path):
    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp s3://{bucket}/{key} '{local_path}'"
        ),
    ]
    subprocess.run(cmd, check=True)


def aws_cp_to_s3(local_path, bucket, key):
    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 cp '{local_path}' s3://{bucket}/{key}"
        ),
    ]
    subprocess.run(cmd, check=True)


def format_thousands_dot(v, _pos=None):
    try:
        return f"{int(round(v)):,}".replace(",", ".")
    except Exception:
        return str(v)


def render_histogram(values, counts, title, subtitle, out_file, bin_edges):
    centers = [(bin_edges[i] + bin_edges[i + 1]) / 2.0 for i in range(len(bin_edges) - 1)]
    width = (bin_edges[1] - bin_edges[0]) * 0.92 if len(bin_edges) > 1 else 1.0

    plt.style.use("seaborn-v0_8-darkgrid")
    fig, ax = plt.subplots(1, 1, figsize=(14.0, 9.0), facecolor="#f6f9ff")
    ax.set_facecolor("#eef4ff")
    ax.bar(centers, counts, width=width, color="#3b82f6", edgecolor="#1e3a8a", linewidth=0.6)
    ax.set_title(title, fontsize=22, fontweight="bold", color="#0f172a")
    ax.set_xlabel("Fare Amount (USD $)", fontsize=16)
    ax.set_ylabel("Number of records", fontsize=16)
    ax.tick_params(axis="both", labelsize=13)
    ax.yaxis.set_major_formatter(FuncFormatter(format_thousands_dot))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=10, integer=True, min_n_ticks=5))
    ax.set_xticks(bin_edges)
    ax.set_xlim(left=bin_edges[0], right=bin_edges[-1])
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    fig.text(0.5, 0.02, subtitle, ha="center", va="bottom", fontsize=11, color="#0f172a")
    plt.tight_layout(rect=[0.01, 0.04, 0.995, 0.985])
    plt.savefig(out_file, dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    generated_key = f"{args.state_prefix}/last_generated_summary.json"
    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_hist_")
    generated_file = os.path.join(tmpdir, "last_generated_summary.json")
    calculation_file = os.path.join(tmpdir, "calculation_summary.json")
    chart_file = os.path.join(tmpdir, "inrange.png")
    summary_file = os.path.join(tmpdir, "summary.json")

    aws_cp_from_s3(args.bucket, generated_key, generated_file)
    with open(generated_file, "r", encoding="utf-8") as f:
        generated = json.load(f)

    calculation_key = str(generated.get("calculation_key") or "")
    if not calculation_key:
        raise ValueError("Missing calculation_key in last_generated_summary")

    aws_cp_from_s3(args.bucket, calculation_key, calculation_file)
    with open(calculation_file, "r", encoding="utf-8") as f:
        calculation = json.load(f)

    snapshot_id = str(calculation.get("snapshot_id") or generated.get("snapshot_id") or "")
    mode = str(calculation.get("mode") or generated.get("mode") or "")
    values = calculation.get("values") or []
    counts = calculation.get("counts") or []
    bin_edges = calculation.get("bin_edges") or [0, 2]
    row_count = int(calculation.get("row_count") or len(values))

    render_histogram(
        values,
        counts,
        title=f"Realtime Fare Amount Histogram Demo ({snapshot_id})",
        subtitle=f"mode={mode} | folder={snapshot_id} | rows={row_count} | values={values}",
        out_file=chart_file,
        bin_edges=bin_edges,
    )

    summary = dict(calculation)
    summary["generated_at"] = datetime.now(timezone.utc).isoformat()
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    base_key = f"{args.snapshot_prefix}/{snapshot_id}/fare_amount"
    aws_cp_to_s3(chart_file, args.bucket, f"{base_key}/inrange.png")
    aws_cp_to_s3(summary_file, args.bucket, f"{base_key}/summary.json")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
