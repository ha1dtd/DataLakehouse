import argparse
import json
import logging
import uuid
from datetime import datetime, timezone

import boto3
import pika
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from hdos_widget_config import (
    JSON_EXPORT_BASE,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RABBITMQ_HOST,
    RABBITMQ_PASS,
    RABBITMQ_PORT,
    RABBITMQ_QUEUE,
    RABBITMQ_USER,
    RABBITMQ_VHOST,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hdos_widget_publish_snapshot_event")


def parse_s3a_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3a://"):
        raise ValueError(f"JSON_EXPORT_BASE must start with s3a://, got: {uri}")
    remainder = uri[len("s3a://") :]
    parts = remainder.split("/", 1)
    bucket = parts[0].strip()
    prefix = parts[1].strip("/") if len(parts) > 1 else ""
    if not bucket:
        raise ValueError(f"Invalid S3A bucket in JSON_EXPORT_BASE: {uri}")
    return bucket, prefix


def snapshot_object_key(object_id: str) -> str:
    _, prefix = parse_s3a_uri(JSON_EXPORT_BASE)
    suffix = f"screen/{object_id}.json"
    return f"{prefix}/{suffix}" if prefix else suffix


def snapshot_uri(object_id: str) -> str:
    bucket, _ = parse_s3a_uri(JSON_EXPORT_BASE)
    return f"s3a://{bucket}/{snapshot_object_key(object_id)}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def load_snapshot_payload(object_id: str) -> dict:
    bucket, _ = parse_s3a_uri(JSON_EXPORT_BASE)
    object_key = snapshot_object_key(object_id)
    logger.info("LOAD_SNAPSHOT bucket=%s key=%s", bucket, object_key)
    client = build_s3_client()
    try:
        response = client.get_object(Bucket=bucket, Key=object_key)
    except ClientError as exc:
        raise RuntimeError(f"Failed to read snapshot object s3a://{bucket}/{object_key}: {exc}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Storage client failed while reading s3a://{bucket}/{object_key}: {exc}") from exc

    payload = json.loads(response["Body"].read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Snapshot object must contain a JSON object: s3a://{bucket}/{object_key}")
    return payload


def build_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=int(RABBITMQ_PORT),
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def build_event(screen_id: str, object_id: str, payload: dict) -> dict:
    bucket, _ = parse_s3a_uri(JSON_EXPORT_BASE)
    object_key = snapshot_object_key(object_id)
    return {
        "event_type": "screen_snapshot_ready",
        "event_id": str(uuid.uuid4()),
        "generated_at": utc_now(),
        "producer": "lakehouse.hdos_widget",
        "version": "1",
        "screen_id": screen_id,
        "object_id": object_id,
        "format": object_id,
        "artifact_uri": snapshot_uri(object_id),
        "bucket": bucket,
        "object_key": object_key,
        "payload": payload,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish hdos_widget FE snapshot event to RabbitMQ")
    parser.add_argument("--screen-id", default="dashboard")
    parser.add_argument("--object-id", default="dashboard_fe")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = load_snapshot_payload(args.object_id)
    event = build_event(args.screen_id, args.object_id, payload)
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")

    logger.info(
        "PUBLISH_EVENT queue=%s screen_id=%s object_id=%s payload_bytes=%s",
        RABBITMQ_QUEUE,
        args.screen_id,
        args.object_id,
        len(body),
    )

    conn = build_connection()
    channel = conn.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    try:
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=event["event_id"],
                timestamp=int(datetime.now(timezone.utc).timestamp()),
            ),
        )
    finally:
        conn.close()

    logger.info(
        "PUBLISH_COMPLETE queue=%s screen_id=%s object_id=%s artifact_uri=%s",
        RABBITMQ_QUEUE,
        args.screen_id,
        args.object_id,
        event["artifact_uri"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
