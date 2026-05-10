"""MinIO/S3-compatible object storage."""
import io
from minio import Minio
from minio.error import S3Error

from common.config.settings import get_settings

settings = get_settings()


def get_minio() -> Minio:
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=False,
    )


def ensure_bucket(bucket: str | None = None) -> None:
    client = get_minio()
    name = bucket or settings.minio_bucket
    try:
        if not client.bucket_exists(name):
            client.make_bucket(name)
    except S3Error:
        raise


def put_object(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    client = get_minio()
    ensure_bucket()
    client.put_object(
        settings.minio_bucket,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )
    return f"s3://{settings.minio_bucket}/{key}"
