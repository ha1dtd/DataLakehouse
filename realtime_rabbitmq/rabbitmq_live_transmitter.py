import argparse
import base64
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen

import pika

DEFAULT_HOST = os.environ.get("FOXAI_RABBITMQ_HOST", "192.168.100.60")
DEFAULT_PORT = int(os.environ.get("FOXAI_RABBITMQ_PORT", "5672"))
DEFAULT_VHOST = os.environ.get("FOXAI_RABBITMQ_VHOST", "/")
DEFAULT_USER = os.environ.get("FOXAI_RABBITMQ_USER", "guest")
DEFAULT_PASS = os.environ.get("FOXAI_RABBITMQ_PASS", "guest")
DEFAULT_QUEUE = os.environ.get("FOXAI_RABBITMQ_QUEUE", "daihai_local_test_1")
DEFAULT_DATASET = os.environ.get("FOXAI_RABBITMQ_DATASET", "fare_amount_realtime_demo")


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


def read_source(source: str) -> tuple[bytes, str, str]:
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        with urlopen(source) as response:
            return response.read(), source, os.path.basename(parsed.path) or "remote.json"
    path = Path(source).expanduser().resolve()
    return path.read_bytes(), str(path), path.name


def build_file_message(source: str) -> dict:
    raw_bytes, resolved_source, source_name = read_source(source)
    ext = Path(source_name).suffix.lower()
    sha256 = hashlib.sha256(raw_bytes).hexdigest()
    event_id = f"file-{sha256[:16]}"
    return {
        "message_type": "file",
        "event_id": event_id,
        "event_ts": utc_now(),
        "dataset": DEFAULT_DATASET,
        "source_type": "url" if urlparse(source).scheme in {"http", "https"} else "local_path",
        "content_encoding": "utf-8",
        "content_base64": base64.b64encode(raw_bytes).decode("ascii"),
        "metadata": {
            "file_event_id": event_id,
            "source_path": resolved_source,
            "source_name": source_name,
            "source_basename": Path(source_name).stem,
            "file_extension": ext,
            "content_sha256": sha256,
            "size_bytes": len(raw_bytes),
        },
    }


def build_row_message(source: str) -> dict:
    path = Path(source).expanduser().resolve()
    row = json.loads(path.read_text(encoding="utf-8"))
    event_id = str(row.get("event_id") or f"row-{uuid.uuid4()}")
    return {
        "message_type": "row",
        "event_id": event_id,
        "event_ts": utc_now(),
        "dataset": DEFAULT_DATASET,
        "source_type": "local_path",
        "row": row,
        "metadata": {
            "source_path": str(path),
            "source_name": path.name,
            "source_basename": path.stem,
            "file_extension": path.suffix.lower(),
        },
    }


def build_text_payload(text: str) -> dict:
    return {
        "message_type": "text",
        "event_id": f"text-{uuid.uuid4()}",
        "event_ts": utc_now(),
        "dataset": DEFAULT_DATASET,
        "payload": {"text": text.strip() or "hello"},
        "metadata": {"source_name": "interactive-shell"},
    }


def send_message(channel, payload: dict) -> dict:
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
        "message_type": payload.get("message_type"),
        "event_id": payload.get("event_id"),
        "metadata": payload.get("metadata"),
    }
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return result


def interactive_shell(channel) -> int:
    print(
        "RabbitMQ transmitter shell -> /file <path|url> | /row <path> | /quit",
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

        stripped = text.strip()
        if stripped in {"/quit", "/exit"}:
            return 0
        if not stripped:
            continue
        if stripped.startswith("/file "):
            send_message(channel, build_file_message(stripped[6:].strip()))
            continue
        if stripped.startswith("/row "):
            send_message(channel, build_row_message(stripped[5:].strip()))
            continue
        send_message(channel, build_text_payload(stripped))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file")
    parser.add_argument("--row")
    args = parser.parse_args(argv)

    conn = build_connection()
    channel = conn.channel()
    channel.queue_declare(queue=DEFAULT_QUEUE, durable=True)
    try:
        if args.file:
            send_message(channel, build_file_message(args.file))
            return 0
        if args.row:
            send_message(channel, build_row_message(args.row))
            return 0
        return interactive_shell(channel)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except (FileNotFoundError, URLError, json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
