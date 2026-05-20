import json
import os
from pathlib import Path
from typing import Any, Final

CONFIG_FILE_ENV = "FOXAI_CONFIG_FILE"
DEFAULT_CONFIG_FILE = Path(__file__).with_suffix(".json")


def _load_config() -> dict[str, Any]:
    config_path = Path(os.environ.get(CONFIG_FILE_ENV, str(DEFAULT_CONFIG_FILE)))
    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)
    if not isinstance(config, dict):
        raise ValueError(f"Config root must be an object: {config_path}")
    return config


_CONFIG = _load_config()


def _resolve_placeholders(value: str) -> str:
    if "{SCRIPT_BASE}" in value:
        value = value.replace("{SCRIPT_BASE}", str(_CONFIG.get("SCRIPT_BASE", "")))
    return value


def get_config(key: str, env_name: str | None = None) -> str:
    env_names = [f"FOXAI_{key}"]
    if env_name:
        env_names.append(env_name)
    else:
        env_names.append(key)
    for name in env_names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    if key not in _CONFIG:
        raise KeyError(f"Missing config key: {key}")
    value = _CONFIG[key]
    if isinstance(value, str):
        return _resolve_placeholders(value)
    return str(value)


MINIO_ENDPOINT: Final[str] = get_config("MINIO_ENDPOINT")
MINIO_ACCESS_KEY: Final[str] = get_config("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY: Final[str] = get_config("MINIO_SECRET_KEY")
KAFKA_BOOTSTRAP: Final[str] = get_config("KAFKA_BOOTSTRAP")
KAFKA_TOPIC: Final[str] = get_config("KAFKA_TOPIC")
KAFKA_BIN: Final[str] = get_config("KAFKA_BIN")
SPARK_SUBMIT_BIN: Final[str] = get_config("SPARK_SUBMIT_BIN")
SCRIPT_BASE: Final[str] = get_config("SCRIPT_BASE")
DOMAIN_REGISTRY_FILE: Final[str] = get_config("DOMAIN_REGISTRY_FILE")
INGEST_SOURCES_FILE: Final[str] = get_config("INGEST_SOURCES_FILE")
RAW_WAREHOUSE: Final[str] = get_config("RAW_WAREHOUSE")
BRONZE_WAREHOUSE: Final[str] = get_config("BRONZE_WAREHOUSE")
SILVER_WAREHOUSE: Final[str] = get_config("SILVER_WAREHOUSE")
GOLD_WAREHOUSE: Final[str] = get_config("GOLD_WAREHOUSE")
ERROR_BUCKET: Final[str] = get_config("ERROR_BUCKET")
ERROR_PREFIX_RAW: Final[str] = get_config("ERROR_PREFIX_RAW", env_name="ERROR_PREFIX")
ERROR_PREFIX_BRONZE: Final[str] = get_config("ERROR_PREFIX_BRONZE", env_name="ERROR_PREFIX")
MAX_MESSAGES: Final[int] = int(get_config("MAX_MESSAGES"))
