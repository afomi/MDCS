from __future__ import annotations

import csv
import io
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict

import pyarrow.parquet as pq

from .adapters import (
    CSVConversationAdapter,
    JSONConversationArrayAdapter,
    JSONLConversationsAdapter,
    ParquetConversationAdapter,
)

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    import gzip
except ImportError:  # pragma: no cover
    gzip = None

try:  # pragma: no cover - optional dependency
    import ijson
except ImportError:  # pragma: no cover
    ijson = None

MAX_SAMPLE_CHARS = 600


def collect_download_preview(artifact) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
    """Return lightweight preview metadata for a downloaded artifact.

    The result is safe for JSON serialization and trimmed to avoid huge payloads.
    """

    file_field = artifact.file
    if not file_field:
        return {}

    adapter_key = artifact.dataset_import.adapter
    storage = file_field.field.storage
    path = file_field.name

    try:
        if adapter_key == ParquetConversationAdapter.slug or path.endswith(".parquet"):
            return _preview_parquet(storage, path)
        if adapter_key == JSONLConversationsAdapter.slug or path.endswith(".jsonl") or path.endswith(".jsonl.gz"):
            return _preview_jsonl(storage, path)
        if adapter_key == JSONConversationArrayAdapter.slug or path.endswith(".json"):
            return _preview_json_array(storage, path)
        if adapter_key == CSVConversationAdapter.slug or path.endswith(".csv"):
            return _preview_csv(storage, path)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Failed to build preview for artifact %s: %s", artifact.pk, exc)
        return {"error": str(exc)}

    return {}


def _preview_parquet(storage, path: str) -> Dict[str, Any]:
    with storage.open(path, "rb") as stream:
        parquet_file = pq.ParquetFile(stream)
        metadata = parquet_file.metadata
        row_count = metadata.num_rows if metadata is not None else None
        sample_row = None
        if parquet_file.num_row_groups > 0:
            table = parquet_file.read_row_group(0, columns=None)
            rows = table.to_pylist()
            if rows:
                sample_row = rows[0]

    return _sanitize_preview(row_count=row_count, sample_row=sample_row)


def _preview_jsonl(storage, path: str) -> Dict[str, Any]:
    row_count = 0
    sample = None
    with _open_text_stream(storage, path) as reader:
        for line in reader:
            line = line.strip()
            if not line:
                continue
            row_count += 1
            if sample is None:
                sample = json.loads(line)
    return _sanitize_preview(row_count=row_count, sample_row=sample)


def _preview_json_array(storage, path: str) -> Dict[str, Any]:
    if ijson is None:
        return {
            "note": "Install optional dependency ijson to enable streaming previews for large JSON files.",
        }

    row_count = 0
    sample = None
    with storage.open(path, "rb") as stream:
        for record in ijson.items(stream, "item"):
            row_count += 1
            if sample is None:
                sample = record
    return _sanitize_preview(row_count=row_count, sample_row=sample)


def _preview_csv(storage, path: str) -> Dict[str, Any]:
    row_count = 0
    sample = None
    with storage.open(path, "r") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            row_count += 1
            if sample is None:
                sample = row
    return _sanitize_preview(row_count=row_count, sample_row=sample)


@contextmanager
def _open_text_stream(storage, path: str):
    if path.endswith(".gz"):
        if gzip is None:
            raise RuntimeError("gzip module not available to read compressed artifact")
        raw = storage.open(path, "rb")
        gz_stream = gzip.GzipFile(fileobj=raw)
        reader = io.TextIOWrapper(gz_stream, encoding="utf-8")
        try:
            yield reader
        finally:
            reader.close()
            gz_stream.close()
            raw.close()
    else:
        reader = storage.open(path, "r")
        try:
            yield reader
        finally:
            reader.close()


def _sanitize_preview(*, row_count: Any = None, sample_row: Any = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if row_count is not None:
        try:
            payload["row_count"] = int(row_count)
        except (TypeError, ValueError):
            payload["row_count"] = row_count
    if sample_row is not None:
        payload["sample_row"] = _stringify(sample_row)
    return payload


def _stringify(value: Any) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        text = str(value)
    if len(text) > MAX_SAMPLE_CHARS:
        return f"{text[:MAX_SAMPLE_CHARS]}…"
    return text
