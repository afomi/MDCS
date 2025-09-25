from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import requests
from django.core.files.storage import Storage
from django.utils import timezone

from .models import DatasetArtifact

DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB
PROGRESS_MIN_INTERVAL = 1.0  # seconds
MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024 * 1024  # 20 GB


@dataclass
class DownloadResult:
    path: str
    size_bytes: int
    checksum: str
    content_type: str | None


class DownloadError(Exception):
    """Raised when a remote artifact cannot be downloaded."""


def stream_download(
    artifact: DatasetArtifact,
    *,
    chunk_callback: Callable[[int, Optional[int]], None],
    timeout: tuple[int, int] = (10, 60),
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> DownloadResult:
    """Stream a remote artifact to storage while reporting incremental progress."""

    url = artifact.download_url
    headers: Dict[str, str] = artifact.metadata.get("headers", {}).copy()
    token = artifact.metadata.get("hf_token")
    if token and "authorization" not in {k.lower() for k in headers}:
        headers["Authorization"] = f"Bearer {token}"

    params = artifact.metadata.get("params") or None

    try:
        response = requests.get(url, headers=headers, params=params, stream=True, timeout=timeout)
    except requests.RequestException as exc:  # pragma: no cover - requests error path
        raise DownloadError(f"Failed to open stream for {url}: {exc}") from exc

    if response.status_code >= 400:
        raise DownloadError(f"Download failed ({response.status_code}) for {url}")

    total = response.headers.get("Content-Length")
    total_bytes = int(total) if total else None
    if total_bytes and total_bytes > MAX_FILE_SIZE_BYTES:
        response.close()
        raise DownloadError(
            f"Artifact exceeds size limit (got {total_bytes} bytes, max {MAX_FILE_SIZE_BYTES})"
        )

    storage = _get_storage(artifact)
    path = artifact.file.name if artifact.file else None
    if not path:
        path = artifact.file.field.upload_to(artifact, os.path.basename(artifact.filename))  # type: ignore[attr-defined]

    ensure_storage_dir(storage, path)

    hasher = hashlib.sha256()
    downloaded = 0
    last_report = timezone.now()

    with storage.open(path, "wb") as destination:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if not chunk:
                continue
            destination.write(chunk)
            hasher.update(chunk)
            downloaded += len(chunk)

            now = timezone.now()
            if (downloaded == total_bytes) or ((now - last_report).total_seconds() >= PROGRESS_MIN_INTERVAL):
                chunk_callback(downloaded, total_bytes)
                last_report = now

            if downloaded > MAX_FILE_SIZE_BYTES:
                response.close()
                raise DownloadError(
                    f"Artifact exceeded size limit during streaming (>{MAX_FILE_SIZE_BYTES} bytes)"
                )

    response.close()
    chunk_callback(downloaded, total_bytes)

    return DownloadResult(
        path=path,
        size_bytes=downloaded,
        checksum=hasher.hexdigest(),
        content_type=response.headers.get("Content-Type"),
    )


def _get_storage(artifact: DatasetArtifact) -> Storage:
    field = artifact._meta.get_field("file")  # type: ignore[arg-type]
    storage: Storage = field.storage  # type: ignore[assignment]
    return storage




def ensure_storage_dir(storage: Storage, name: str) -> None:
    if hasattr(storage, "path"):
        full_path = storage.path(name)
        directory = os.path.dirname(full_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
