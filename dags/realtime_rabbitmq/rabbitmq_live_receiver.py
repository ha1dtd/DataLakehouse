import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

import pika
from urllib.request import Request, urlopen

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://192.168.100.66:9001")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "12345678")
AIRFLOW_API_BASE = "http://192.168.100.66:8081/api/v1"
AIRFLOW_USER = "admin"
AIRFLOW_PASS = "admin"
AIRFLOW_DAG_ID = os.environ.get("FOXAI_AIRFLOW_DAG_ID", "realtime_rabbitmq")
DEFAULT_BUCKET = os.environ.get("FOXAI_MINIO_BUCKET", "histogram")
DEFAULT_EVENT_PREFIX = os.environ.get("FOXAI_EVENT_PREFIX", "demo/realtime_rabbitmq_fare_amount/event")
DEFAULT_HOST = os.environ.get("FOXAI_RABBITMQ_HOST", "192.168.100.60")
DEFAULT_PORT = int(os.environ.get("FOXAI_RABBITMQ_PORT", "5672"))
DEFAULT_VHOST = os.environ.get("FOXAI_RABBITMQ_VHOST", "/")
DEFAULT_USER = os.environ.get("FOXAI_RABBITMQ_USER", "guest")
DEFAULT_PASS = os.environ.get("FOXAI_RABBITMQ_PASS", "guest")
DEFAULT_QUEUE = os.environ.get("FOXAI_RABBITMQ_QUEUE", "daihai_local_test_1")
DEFAULT_RECONNECT_SEC = int(os.environ.get("FOXAI_RABBITMQ_RECONNECT_SEC", "5"))

SHOULD_STOP = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def log(event: dict, *, stream=sys.stdout) -> None:
    print(json.dumps(event, ensure_ascii=False), file=stream, flush=True)


def build_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(DEFAULT_USER, DEFAULT_PASS)
    params = pika.ConnectionParameters(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        virtual_host=DEFAULT_VHOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def handle_stop(signum, frame) -> None:
    del signum, frame
    global SHOULD_STOP
    SHOULD_STOP = True


def decode_payload(body: bytes) -> dict:
    raw_text = body.decode("utf-8", errors="replace")
    return json.loads(raw_text)


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


def persist_event(payload: dict) -> str:
    event_id = str(payload.get("event_id") or f"event-{utc_now_compact()}")
    message_type = str(payload.get("message_type") or "unknown")
    date_prefix = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    key = f"{DEFAULT_EVENT_PREFIX}/{message_type}/{date_prefix}/{event_id}.json"
    tmpdir = tempfile.mkdtemp(prefix="rabbitmq_event_")
    event_file = os.path.join(tmpdir, "event.json")
    with open(event_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    aws_cp_to_s3(event_file, DEFAULT_BUCKET, key)
    return key


def trigger_dag(event_key: str, payload: dict) -> dict:
    conf = {
        "event_key": event_key,
        "event_id": payload.get("event_id"),
        "message_type": payload.get("message_type"),
        "dataset": payload.get("dataset"),
    }
    request = Request(
        url=f"{AIRFLOW_API_BASE.rstrip('/')}/dags/{AIRFLOW_DAG_ID}/dagRuns",
        data=json.dumps({"conf": conf}).encode("utf-8"),
        headers={
            "Authorization": "Basic " + __import__("base64").b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode("utf-8")).decode("ascii"),
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def main() -> int:
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    log(
        {
            "status": "starting",
            "queue": DEFAULT_QUEUE,
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "vhost": DEFAULT_VHOST,
            "started_at": utc_now(),
            "airflow_dag_id": AIRFLOW_DAG_ID,
        }
    )

    while not SHOULD_STOP:
        conn = None
        try:
            conn = build_connection()
            channel = conn.channel()
            channel.queue_declare(queue=DEFAULT_QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)
            log({"status": "waiting", "queue": DEFAULT_QUEUE, "at": utc_now()})

            while not SHOULD_STOP:
                method_frame, header_frame, body = channel.basic_get(queue=DEFAULT_QUEUE, auto_ack=False)
                if method_frame is None:
                    conn.process_data_events(time_limit=1)
                    time.sleep(0.2)
                    continue

                payload = decode_payload(body)
                message_type = str(payload.get("message_type") or "")
                if message_type not in {"file", "row"}:
                    log(
                        {
                            "status": "ignored",
                            "queue": DEFAULT_QUEUE,
                            "message_id": getattr(header_frame, "message_id", None),
                            "message_type": message_type,
                            "reason": "unsupported-message-type",
                        }
                    )
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    continue

                event_key = persist_event(payload)
                trigger_response = trigger_dag(event_key, payload)
                log(
                    {
                        "status": "received",
                        "received_at": utc_now(),
                        "queue": DEFAULT_QUEUE,
                        "delivery_tag": method_frame.delivery_tag,
                        "message_id": getattr(header_frame, "message_id", None),
                        "event_id": payload.get("event_id"),
                        "message_type": message_type,
                        "event_key": event_key,
                        "dag_run_id": trigger_response.get("dag_run_id") or trigger_response.get("run_id"),
                    }
                )
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as exc:
            log(
                {
                    "status": "connection-error",
                    "at": utc_now(),
                    "queue": DEFAULT_QUEUE,
                    "error": str(exc),
                    "retry_in_sec": DEFAULT_RECONNECT_SEC,
                },
                stream=sys.stderr,
            )
            if SHOULD_STOP:
                break
            time.sleep(DEFAULT_RECONNECT_SEC)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    log({"status": "stopped", "queue": DEFAULT_QUEUE, "stopped_at": utc_now()})
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
