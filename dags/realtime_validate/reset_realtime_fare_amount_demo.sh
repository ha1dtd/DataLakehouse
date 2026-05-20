#!/usr/bin/env bash
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://192.168.100.66:9001}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-admin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-12345678}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-192.168.100.66:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-realtime_fare_amount_demo}"
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-realtime-fare-amount-demo-airflow}"
KAFKA_BIN="${KAFKA_BIN:-/opt/confluent-7.8.0/bin}"
STATE_PREFIX="demo/realtime_fare_amount/state"
SNAPSHOT_PREFIX="demo/realtime_fare_amount"
BUCKET="histogram"

export AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY"

aws --endpoint-url "$MINIO_ENDPOINT" s3 rm "s3://$BUCKET/$STATE_PREFIX/" --recursive || true
aws --endpoint-url "$MINIO_ENDPOINT" s3 rm "s3://$BUCKET/$SNAPSHOT_PREFIX/" --recursive || true
"$KAFKA_BIN/kafka-consumer-groups" --bootstrap-server "$KAFKA_BOOTSTRAP" --group "$KAFKA_GROUP_ID" --topic "$KAFKA_TOPIC" --reset-offsets --to-earliest --execute || true

echo "Reset done: MinIO demo state/snapshots removed, Kafka group offsets rewound to earliest."
