import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone

MINIO_ENDPOINT = "http://192.168.100.66:9001"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "12345678"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "192.168.100.66:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "realtime_fare_amount_demo")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "realtime-fare-amount-demo-airflow")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/opt/confluent/bin")


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


def aws_exists(bucket, key):
    cmd = [
        "bash",
        "-lc",
        (
            f"AWS_ACCESS_KEY_ID='{MINIO_ACCESS_KEY}' "
            f"AWS_SECRET_ACCESS_KEY='{MINIO_SECRET_KEY}' "
            f"aws --endpoint-url {MINIO_ENDPOINT} s3 ls s3://{bucket}/{key} >/dev/null 2>&1"
        ),
    ]
    result = subprocess.run(cmd)
    return result.returncode == 0


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_messages(stdout_text):
    lines = [line.strip() for line in stdout_text.splitlines() if line.strip()]
    messages = []
    for line in lines:
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return messages


def consume_messages(max_messages, timeout_ms, bootstrap_from_beginning=False):
    cmd = [
        f"{KAFKA_BIN}/kafka-console-consumer",
        "--bootstrap-server",
        KAFKA_BOOTSTRAP,
        "--topic",
        KAFKA_TOPIC,
    ]
    if bootstrap_from_beginning:
        cmd.extend([
            "--from-beginning",
            "--max-messages",
            str(max_messages),
            "--timeout-ms",
            str(timeout_ms),
        ])
    else:
        cmd.extend([
            "--group",
            KAFKA_GROUP_ID,
            "--consumer-property",
            "auto.offset.reset=earliest",
            "--max-messages",
            str(max_messages),
            "--timeout-ms",
            str(timeout_ms),
        ])
    result = subprocess.run(cmd, capture_output=True, text=True)
    return parse_messages(result.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="histogram")
    parser.add_argument("--state-prefix", default="demo/realtime_fare_amount/state")
    parser.add_argument("--max-messages", type=int, default=100)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    args = parser.parse_args()

    state_key = f"{args.state_prefix}/current_rows.json"
    event_key = f"{args.state_prefix}/processed_event_ids.json"
    batch_key = f"{args.state_prefix}/processed_batch_ids.json"
    summary_key = f"{args.state_prefix}/last_consume_summary.json"

    tmpdir = tempfile.mkdtemp(prefix="realtime_fare_consume_")
    state_file = os.path.join(tmpdir, "current_rows.json")
    event_file = os.path.join(tmpdir, "processed_event_ids.json")
    batch_file = os.path.join(tmpdir, "processed_batch_ids.json")
    summary_file = os.path.join(tmpdir, "last_consume_summary.json")

    rows = []
    processed_event_ids = []
    processed_batch_ids = []

    state_exists = aws_exists(args.bucket, state_key)
    event_exists = aws_exists(args.bucket, event_key)
    batch_exists = aws_exists(args.bucket, batch_key)

    if state_exists:
        aws_cp_from_s3(args.bucket, state_key, state_file)
        with open(state_file, "r", encoding="utf-8") as f:
            rows = json.load(f)

    if event_exists:
        aws_cp_from_s3(args.bucket, event_key, event_file)
        with open(event_file, "r", encoding="utf-8") as f:
            processed_event_ids = json.load(f)

    if batch_exists:
        aws_cp_from_s3(args.bucket, batch_key, batch_file)
        with open(batch_file, "r", encoding="utf-8") as f:
            processed_batch_ids = json.load(f)

    bootstrap_from_beginning = not state_exists and not event_exists and not batch_exists
    messages = consume_messages(
        args.max_messages,
        args.timeout_ms,
        bootstrap_from_beginning=bootstrap_from_beginning,
    )
    applied = 0

    for message in messages:
        message_type = message.get("message_type")
        if message_type == "batch":
            batch_id = str(message.get("batch_id") or "")
            if not batch_id or batch_id in processed_batch_ids:
                continue
            for row in message.get("rows", []):
                event_id = str(row.get("event_id") or row.get("trip_id") or "")
                if event_id and event_id not in processed_event_ids:
                    rows.append(row)
                    processed_event_ids.append(event_id)
                    applied += 1
            processed_batch_ids.append(batch_id)
        elif message_type == "row":
            row = message.get("row") or {}
            event_id = str(message.get("event_id") or row.get("event_id") or row.get("trip_id") or "")
            if not event_id or event_id in processed_event_ids:
                continue
            rows.append(row)
            processed_event_ids.append(event_id)
            applied += 1

    summary = {
        "consumed_at": utc_now_iso(),
        "message_count": len(messages),
        "applied_row_count": applied,
        "total_row_count_after": len(rows),
        "should_generate_histogram": applied > 0,
        "bootstrap_from_beginning": bootstrap_from_beginning,
    }

    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    with open(event_file, "w", encoding="utf-8") as f:
        json.dump(processed_event_ids, f, indent=2)
    with open(batch_file, "w", encoding="utf-8") as f:
        json.dump(processed_batch_ids, f, indent=2)
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    aws_cp_to_s3(state_file, args.bucket, state_key)
    aws_cp_to_s3(event_file, args.bucket, event_key)
    aws_cp_to_s3(batch_file, args.bucket, batch_key)
    aws_cp_to_s3(summary_file, args.bucket, summary_key)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
