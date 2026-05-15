import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika

DEFAULT_HOST = os.environ.get("FOXAI_RABBITMQ_HOST", "192.168.100.60")
DEFAULT_PORT = int(os.environ.get("FOXAI_RABBITMQ_PORT", "5672"))
DEFAULT_VHOST = os.environ.get("FOXAI_RABBITMQ_VHOST", "/")
DEFAULT_USER = os.environ.get("FOXAI_RABBITMQ_USER", "guest")
DEFAULT_PASS = os.environ.get("FOXAI_RABBITMQ_PASS", "guest")
DEFAULT_QUEUE = os.environ.get("FOXAI_RABBITMQ_OUT_QUEUE", "haibigdhoangsmalld")
DEFAULT_INTERVAL_SEC = int(os.environ.get("FOXAI_RABBITMQ_INTERVAL_SEC", "5"))
DEFAULT_MESSAGES_FILE = Path(os.environ.get("FOXAI_RABBITMQ_MESSAGES_FILE", Path(__file__).with_name("rabbitmq_publish_messages.json")))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def load_messages(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Messages file must contain a JSON array: {path}")
    normalized = []
    for i, item in enumerate(data, start=1):
        if isinstance(item, dict):
            normalized.append(item)
        else:
            normalized.append({"message": item, "sequence": i})
    return normalized


def main() -> int:
    messages = load_messages(DEFAULT_MESSAGES_FILE)
    conn = build_connection()
    channel = conn.channel()
    channel.queue_declare(queue=DEFAULT_QUEUE, durable=True)

    total = len(messages)
    for idx, payload in enumerate(messages, start=1):
        envelope = {
            "publisher_run_id": str(uuid.uuid4()),
            "published_at": utc_now(),
            "sequence": idx,
            "total": total,
            "payload": payload,
        }
        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        channel.basic_publish(
            exchange="",
            routing_key=DEFAULT_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=str(uuid.uuid4()),
                timestamp=int(datetime.now(timezone.utc).timestamp()),
            ),
        )
        print(json.dumps({"status": "published", "queue": DEFAULT_QUEUE, "sequence": idx, "total": total, "payload": envelope}, ensure_ascii=False))
        if idx < total:
            time.sleep(DEFAULT_INTERVAL_SEC)

    conn.close()
    print(json.dumps({"status": "done", "queue": DEFAULT_QUEUE, "count": total}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
