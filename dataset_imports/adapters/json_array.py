from __future__ import annotations

import json
from typing import Iterable, List

from ..models import DatasetArtifact
from ..normalization import NormalizedConversation
from .base import DatasetAdapter

try:  # pragma: no cover - optional import
    import ijson
except ImportError:  # pragma: no cover
    ijson = None


class JSONConversationArrayAdapter(DatasetAdapter):
    """Parses large JSON arrays of conversations (e.g., ShareGPT)."""

    slug = "json-conversation-array"

    def process(self, artifact: DatasetArtifact, path: str) -> Iterable[NormalizedConversation]:
        if ijson is not None:
            with self.open_artifact(path, mode="rb") as stream:
                for record in ijson.items(stream, "item"):
                    yield from self._normalize(record)
        else:
            with self.open_artifact(path, mode="r") as stream:
                data = json.load(stream)
                for record in data:
                    yield from self._normalize(record)

    def _normalize(self, record: dict) -> List[NormalizedConversation]:
        from .jsonl import JSONLConversationsAdapter

        helper = JSONLConversationsAdapter(self.storage)
        return helper._normalize_record(record)

