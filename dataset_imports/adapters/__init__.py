from __future__ import annotations

from django.core.files.storage import Storage

from .base import DatasetAdapter
from .csv_adapter import CSVConversationAdapter
from .json_array import JSONConversationArrayAdapter
from .jsonl import JSONLConversationsAdapter
from .parquet_adapter import ParquetConversationAdapter
from .oasst import OASSTMessagesAdapter

ADAPTER_REGISTRY = {
    JSONLConversationsAdapter.slug: JSONLConversationsAdapter,
    JSONConversationArrayAdapter.slug: JSONConversationArrayAdapter,
    CSVConversationAdapter.slug: CSVConversationAdapter,
    ParquetConversationAdapter.slug: ParquetConversationAdapter,
    OASSTMessagesAdapter.slug: OASSTMessagesAdapter,
}


def get_adapter(adapter_key: str, storage: Storage) -> DatasetAdapter:
    try:
        adapter_cls = ADAPTER_REGISTRY[adapter_key]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset adapter '{adapter_key}'") from exc
    return adapter_cls(storage)


__all__ = [
    "get_adapter",
    "DatasetAdapter",
]

