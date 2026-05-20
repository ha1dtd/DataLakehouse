import json
import os
import sys
import uuid
from datetime import datetime, timezone

import pika

DEFAULT_HOST = os.environ.get("FOXAI_RABBITMQ_HOST", "192.168.100.60")
DEFAULT_PORT = int(os.environ.get("FOXAI_RABBITMQ_PORT", "5672"))
DEFAULT_VHOST = os.environ.get("FOXAI_RABBITMQ_VHOST", "/")
DEFAULT_USER = os.environ.get("FOXAI_RABBITMQ_USER", "guest")
DEFAULT_PASS = os.environ.get("FOXAI_RABBITMQ_PASS", "guest")
DEFAULT_IN_QUEUE = os.environ.get("FOXAI_RABBITMQ_IN_QUEUE", "order.create-requested")
DEFAULT_OUT_QUEUE = os.environ.get("FOXAI_RABBITMQ_OUT_QUEUE", "notification.send-requested")
DEFAULT_TIMEOUT = int(os.environ.get("FOXAI_RABBITMQ_TIMEOUT_SEC", "30"))


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


def main() -> int:
    conn = build_connection()
    channel = conn.channel()
    channel.queue_declare(queue=DEFAULT_IN_QUEUE, durable=True)
    channel.queue_declare(queue=DEFAULT_OUT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    method_frame, header_frame, body = channel.basic_get(queue=DEFAULT_IN_QUEUE, auto_ack=False)
    if method_frame is None:
        print(
            json.dumps(
                {
                    "status": "empty",
                    "in_queue": DEFAULT_IN_QUEUE,
                    "out_queue": DEFAULT_OUT_QUEUE,
                    "checked_at": utc_now(),
                    "host": DEFAULT_HOST,
                    "port": DEFAULT_PORT,
                },
                ensure_ascii=False,
            )
        )
        conn.close()
        return 0

    raw_text = body.decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        payload = {"raw_body": raw_text}

    response = {
        "relay_id": str(uuid.uuid4()),
        "relay_ts": utc_now(),
        "source_queue": DEFAULT_IN_QUEUE,
        "source_delivery_tag": method_frame.delivery_tag,
        "received_payload": payload,
        "status": "received-and-forwarded",
    }

    channel.basic_publish(
        exchange="",
        routing_key=DEFAULT_OUT_QUEUE,
        body=json.dumps(response, ensure_ascii=False).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            correlation_id=str(getattr(header_frame, "correlation_id", "") or ""),
            message_id=str(uuid.uuid4()),
            timestamp=int(datetime.now(timezone.utc).timestamp()),
        ),
    )
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    print(
        json.dumps(
            {
                "status": "forwarded",
                "in_queue": DEFAULT_IN_QUEUE,
                "out_queue": DEFAULT_OUT_QUEUE,
                "checked_at": utc_now(),
                "received": payload,
                "published": response,
            },
            ensure_ascii=False,
        )
    )
    conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
