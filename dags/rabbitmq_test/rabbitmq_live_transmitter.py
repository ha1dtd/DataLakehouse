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
DEFAULT_QUEUE = os.environ.get("FOXAI_RABBITMQ_QUEUE", "haibigdhoangsmalld")


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


def build_payload(text: str) -> dict:
    normalized = text.strip() or "hello"
    return {
        "sender": "local-test",
        "sent_at": utc_now(),
        "text": normalized,
    }


def send_payload(channel, text: str) -> dict:
    payload = build_payload(text)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    message_id = str(uuid.uuid4())
    channel.basic_publish(
        exchange="",
        routing_key=DEFAULT_QUEUE,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=message_id,
            timestamp=int(datetime.now(timezone.utc).timestamp()),
        ),
    )
    result = {
        "status": "sent",
        "queue": DEFAULT_QUEUE,
        "message_id": message_id,
        "payload": payload,
    }
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


def interactive_shell(channel) -> int:
    print(
        f"RabbitMQ transmitter shell -> queue={DEFAULT_QUEUE}. Type a message. /quit to exit.",
        flush=True,
    )
    while True:
        try:
            text = input("tx> ")
        except EOFError:
            print()
            return 0
        except KeyboardInterrupt:
            print()
            return 130

        if text.strip() in {"/quit", "/exit"}:
            return 0
        if not text.strip():
            continue
        send_payload(channel, text)


def main(argv: list[str]) -> int:
    conn = build_connection()
    channel = conn.channel()
    channel.queue_declare(queue=DEFAULT_QUEUE, durable=True)
    try:
        if argv:
            send_payload(channel, " ".join(argv))
            return 0
        return interactive_shell(channel)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
