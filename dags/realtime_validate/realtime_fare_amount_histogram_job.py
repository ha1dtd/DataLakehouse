import argparse
import json
import math
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


def sampled_xticks(bin_edges, max_ticks=12):
    if len(bin_edges) <= max_ticks:
        return bin_edges
    last_index = len(bin_edges) - 1
    step = max(1, int(math.ceil(last_index / (max_ticks - 1))))
    ticks = [bin_edges[index] for index in range(0, last_index, step)]
    if ticks[-1] != bin_edges[-1]:
        ticks.append(bin_edges[-1])
    return ticks


def render_histogram(counts, title, subtitle, out_file, bin_edges):
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
    ax.set_xticks(sampled_xticks(bin_edges))
    ax.set_xlim(left=bin_edges[0], right=bin_edges[-1])
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    fig.text(0.5, 0.02, subtitle, ha="center", va="bottom", fontsize=11, color="#0f172a")
    plt.tight_layout(rect=[0.01, 0.04, 0.995, 0.985])
    plt.savefig(out_file, dpi=150)
    plt.close(fig)


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_state_keys(state_prefix):
    return {
        "generated": f"{state_prefix}/last_generated_summary.json",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_rabbitmq_fare_amount/state")
    parser.add_argument("--snapshot-prefix", default="demo")
    args = parser.parse_args()

    state_keys = build_state_keys(args.state_prefix)
    tmpdir = tempfile.mkdtemp(prefix="realtime_rabbitmq_hist_")
    generated_file = os.path.join(tmpdir, "last_generated_summary.json")
    calculation_file = os.path.join(tmpdir, "calculation_summary.json")
    chart_file = os.path.join(tmpdir, "inrange.png")
    summary_file = os.path.join(tmpdir, "summary.json")

    aws_cp_from_s3(args.bucket, state_keys["generated"], generated_file)
    generated = load_json(generated_file)

    calculation_key = str(generated.get("calculation_key") or "")
    if not calculation_key:
        raise ValueError("Missing calculation_key in last_generated_summary")

    aws_cp_from_s3(args.bucket, calculation_key, calculation_file)
    calculation = load_json(calculation_file)

    snapshot_id = str(calculation.get("snapshot_id") or generated.get("snapshot_id") or "")
    mode = str(calculation.get("mode") or generated.get("mode") or "")
    counts = calculation.get("counts") or []
    bin_edges = calculation.get("bin_edges") or [0, 2]
    row_count = int(calculation.get("row_count") or 0)

    render_histogram(
        counts,
        title=f"Realtime Fare Amount Histogram Demo ({mode.upper() if mode else 'UNKNOWN'})",
        subtitle=f"snapshot={snapshot_id} | mode={mode} | rows={row_count}",
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
    print(
        json.dumps(
            {
                "status": "ok",
                "feature": summary.get("feature"),
                "mode": summary.get("mode"),
                "snapshot_id": snapshot_id,
                "row_count": summary.get("row_count"),
                "chart_key": f"{base_key}/inrange.png",
                "summary_key": f"{base_key}/summary.json",
            }
        )
    )


if __name__ == "__main__":
    main()
