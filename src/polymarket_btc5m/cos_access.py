from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from qcloud_cos import CosConfig, CosS3Client
from qcloud_cos.cos_exception import CosServiceError


@dataclass(frozen=True)
class CosAccessConfig:
    bucket: str
    region: str
    secret_id: str
    secret_key: str
    scheme: str = "https"


def normalize_prefix(prefix: str | None) -> str:
    cleaned = str(prefix or "").strip().lstrip("/")
    if not cleaned:
        return ""
    return cleaned if cleaned.endswith("/") else f"{cleaned}/"


def build_client(config: CosAccessConfig) -> CosS3Client:
    return CosS3Client(
        CosConfig(
            Region=config.region,
            SecretId=config.secret_id,
            SecretKey=config.secret_key,
            Scheme=config.scheme,
        )
    )


def build_object_url(bucket: str, region: str, key: str, scheme: str = "https") -> str:
    return f"{scheme}://{bucket}.cos.{region}.myqcloud.com/{key.lstrip('/')}"


def list_objects(
    client: CosS3Client,
    bucket: str,
    prefix: str = "",
    *,
    max_items: int | None = None,
) -> Iterator[dict[str, Any]]:
    marker = ""
    yielded = 0
    normalized_prefix = normalize_prefix(prefix)

    while True:
        try:
            response = client.list_objects(
                Bucket=bucket,
                Prefix=normalized_prefix,
                Marker=marker,
                MaxKeys=1000,
            )
        except CosServiceError as exc:
            if exc.get_error_code() == "AccessDenied":
                raise PermissionError(
                    "COS credentials do not have bucket listing permission. "
                    "Use explicit object keys or ask SRE for ListBucket/GetBucket access."
                ) from exc
            raise
        contents = response.get("Contents", []) or []
        for item in contents:
            if not isinstance(item, dict):
                continue
            yield item
            yielded += 1
            if max_items is not None and yielded >= max_items:
                return

        if response.get("IsTruncated") != "true":
            return
        marker = str(response.get("NextMarker") or "")
        if not marker:
            return


def read_object_bytes(client: CosS3Client, bucket: str, key: str) -> bytes:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    return body.get_raw_stream().read()


def download_object_to_path(client: CosS3Client, bucket: str, key: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    response = client.get_object(Bucket=bucket, Key=key)
    response["Body"].get_stream_to_file(str(path))
