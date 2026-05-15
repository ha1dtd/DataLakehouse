import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pika

DEFAULT_HOST = os.environ.get("FOXAI_RABBITMQ_HOST", "192.168.100.60")
DEFAULT_PORT = int(os.environ.get("FOXAI_RABBITMQ_PORT", "5672"))
DEFAULT_VHOST = os.environ.get("FOXAI_RABBITMQ_VHOST", "/")
DEFAULT_USER = os.environ.get("FOXAI_RABBITMQ_USER", "guest")
DEFAULT_PASS = os.environ.get("FOXAI_RABBITMQ_PASS", "guest")
DEFAULT_QUEUE = os.environ.get("FOXAI_RABBITMQ_QUEUE", "haibigdhoangsmalld")
DEFAULT_RECONNECT_SEC = int(os.environ.get("FOXAI_RABBITMQ_RECONNECT_SEC", "5"))

SHOULD_STOP = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        return {"raw_body": raw_text}


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
                log(
                    {
                        "status": "received",
                        "received_at": utc_now(),
                        "queue": DEFAULT_QUEUE,
                        "delivery_tag": method_frame.delivery_tag,
                        "message_id": getattr(header_frame, "message_id", None),
                        "correlation_id": getattr(header_frame, "correlation_id", None),
                        "payload": payload,
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
