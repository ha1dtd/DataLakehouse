import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONFIG_FILE_ENV = "HDOS_API_CONFIG_FILE"
DEFAULT_CONFIG_FILE = Path(__file__).resolve().parent.parent / "hdos_widget_config.json"


def _load_config() -> dict[str, Any]:
    config_path = Path(os.environ.get(CONFIG_FILE_ENV, str(DEFAULT_CONFIG_FILE)))
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    if not isinstance(config, dict):
        raise ValueError(f"Config root must be an object: {config_path}")
    return config


def _get_value(config: dict[str, Any], key: str, default: str = "") -> str:
    env_value = os.environ.get(key)
    if env_value is not None and env_value.strip():
        return env_value.strip()
    config_value = config.get(key, default)
    if isinstance(config_value, str):
        return config_value.strip()
    return str(config_value)


def _parse_s3a_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3a://"):
        raise ValueError(f"JSON_EXPORT_BASE must start with s3a://, got: {uri}")
    remainder = uri[len("s3a://") :]
    parts = remainder.split("/", 1)
    bucket = parts[0].strip()
    prefix = parts[1].strip("/") if len(parts) > 1 else ""
    if not bucket:
        raise ValueError(f"Invalid S3A bucket in JSON_EXPORT_BASE: {uri}")
    return bucket, prefix


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    screen_bucket: str
    screen_prefix: str
    region_name: str = "us-east-1"

    def screen_object_key(self, screen_id: str) -> str:
        screen_id = screen_id.strip().strip("/")
        if not screen_id:
            raise ValueError("screen_id must not be empty")
        suffix = f"screen/{screen_id}.json"
        return f"{self.screen_prefix}/{suffix}" if self.screen_prefix else suffix


def load_settings() -> Settings:
    config = _load_config()
    export_base = _get_value(config, "JSON_EXPORT_BASE")
    default_bucket, default_prefix = _parse_s3a_uri(export_base)
    return Settings(
        minio_endpoint=_get_value(config, "MINIO_ENDPOINT"),
        minio_access_key=_get_value(config, "MINIO_ACCESS_KEY"),
        minio_secret_key=_get_value(config, "MINIO_SECRET_KEY"),
        screen_bucket=os.environ.get("HDOS_SCREEN_BUCKET", default_bucket).strip(),
        screen_prefix=os.environ.get("HDOS_SCREEN_PREFIX", default_prefix).strip("/"),
        region_name=os.environ.get("AWS_REGION", "us-east-1").strip(),
    )
