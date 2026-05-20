import json
import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone

from dags.combined_domains.foxai_config import DOMAIN_REGISTRY_FILE, INGEST_SOURCES_FILE, KAFKA_BIN, KAFKA_BOOTSTRAP, KAFKA_TOPIC

SOURCES_FILE = INGEST_SOURCES_FILE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger(__name__)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


with open(SOURCES_FILE, "r", encoding="utf-8") as f:
    sources = json.load(f)

with open(DOMAIN_REGISTRY_FILE, "r", encoding="utf-8") as f:
    domain_registry = json.load(f)

registered_domains = domain_registry.get("domains", {})
default_domain = domain_registry.get("default_domain", "default")
default_topic = domain_registry.get("default_topic", "general")

messages = []
for src in sources:
    source_uri = src["source_uri"]
    file_name = os.path.basename(source_uri)
    file_type = os.path.splitext(file_name)[1].lstrip(".").lower() or "unknown"
    domain = src.get("domain") or default_domain
    topic = src.get("topic") or default_topic

    if domain not in registered_domains:
        raise ValueError(f"Domain '{domain}' is not registered in {DOMAIN_REGISTRY_FILE}")

    msg = {
        "event_type": "raw_ingest_request",
        "job_id": str(uuid.uuid4()),
        "source_type": src["source_type"],
        "source_uri": source_uri,
        "source_name": src["source_name"],
        "domain": domain,
        "topic": topic,
        "file_name": file_name,
        "file_type": file_type,
        "requested_at": utc_now_iso(),
        "status": "requested"
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
logger.info("ENQUEUED_REQUESTS=%s", len(messages))
