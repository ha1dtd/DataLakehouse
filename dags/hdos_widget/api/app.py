import json
import logging

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

try:
    from .config import Settings, load_settings
except ImportError:  # pragma: no cover - fallback for direct folder execution
    from config import Settings, load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("hdos_widget_api")

app = FastAPI(title="HDOS Widget API", version="1.0.0")
settings = load_settings()


def build_s3_client(current_settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=current_settings.minio_endpoint,
        aws_access_key_id=current_settings.minio_access_key,
        aws_secret_access_key=current_settings.minio_secret_key,
        region_name=current_settings.region_name,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


s3_client = build_s3_client(settings)


def resolve_screen_object_id(screen_id: str) -> str:
    if screen_id == "dashboard":
        return "dashboard_fe"
    return screen_id


def load_screen_payload(screen_id: str) -> dict:
    object_id = resolve_screen_object_id(screen_id)
    object_key = settings.screen_object_key(object_id)
    logger.info(
        "LOAD_SCREEN screen_id=%s resolved_object_id=%s bucket=%s key=%s",
        screen_id,
        object_id,
        settings.screen_bucket,
        object_key,
    )
    try:
        response = s3_client.get_object(Bucket=settings.screen_bucket, Key=object_key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in {"NoSuchKey", "404"}:
            raise HTTPException(status_code=404, detail=f"Screen snapshot not found: {screen_id}") from exc
        logger.exception("MINIO_GET_OBJECT_FAILED screen_id=%s", screen_id)
        raise HTTPException(status_code=502, detail="Failed to read screen snapshot from storage") from exc
    except BotoCoreError as exc:
        logger.exception("MINIO_CLIENT_FAILURE screen_id=%s", screen_id)
        raise HTTPException(status_code=503, detail="Storage client is unavailable") from exc

    try:
        body_bytes = response["Body"].read()
        payload = json.loads(body_bytes.decode("utf-8"))
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.exception("SCREEN_PAYLOAD_INVALID screen_id=%s", screen_id)
        raise HTTPException(status_code=502, detail="Stored screen snapshot is invalid JSON") from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Stored screen snapshot must be a JSON object")
    return payload


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "service": "hdos_widget_api",
        "screen_bucket": settings.screen_bucket,
        "screen_prefix": settings.screen_prefix,
    }


@app.get("/api/screen/{screen_id}")
def get_screen(screen_id: str) -> JSONResponse:
    payload = load_screen_payload(screen_id)
    return JSONResponse(content=payload)
