import json
import os
import subprocess
import uuid
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "192.168.100.66:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_ingest_events")
SOURCES_FILE = os.environ.get("INGEST_SOURCES_FILE", "/home/ubuntu/scripts/ingest_sources_demo.json")
KAFKA_BIN = os.environ.get("KAFKA_BIN", "/opt/confluent/bin")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    sources = json.load(f)

messages = []
for src in sources:
    source_uri = src["source_uri"]
    file_name = os.path.basename(source_uri)
    file_type = os.path.splitext(file_name)[1].lstrip(".").lower() or "unknown"
    msg = {
        "event_type": "raw_ingest_request",
        "job_id": str(uuid.uuid4()),
        "source_type": src["source_type"],
        "source_uri": source_uri,
        "source_name": src["source_name"],
        "file_name": file_name,
        "file_type": file_type,
        "requested_at": utc_now_iso(),
        "status": "requested",
    }
    messages.append(json.dumps(msg))

producer_cmd = [
    f"{KAFKA_BIN}/kafka-console-producer",
    "--bootstrap-server",
    KAFKA_BOOTSTRAP,
    "--topic",
    KAFKA_TOPIC,
]

payload = "\n".join(messages) + "\n"
subprocess.run(producer_cmd, input=payload.encode("utf-8"), check=True)
print(f"ENQUEUED_REQUESTS={len(messages)}")
