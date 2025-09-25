from __future__ import annotations

import io
import json
from collections import defaultdict
from typing import Dict, Iterable, List

from ..models import DatasetArtifact
from ..normalization import NormalizedConversation, NormalizedMessage, normalize_role
from .base import DatasetAdapter

try:  # pragma: no cover - optional dependency
    import gzip
except ImportError:  # pragma: no cover
    gzip = None


class OASSTMessagesAdapter(DatasetAdapter):
    """Normalizes OpenAssistant message JSONL exports."""

    slug = "oasst-messages"

    def process(self, artifact: DatasetArtifact, path: str) -> Iterable[NormalizedConversation]:
        raw = self.open_artifact(path, mode="rb")
        stream: io.TextIOBase
        if path.endswith(".gz"):
            if gzip is None:
                raise RuntimeError("gzip module unavailable for .gz artifact")
            stream = io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8")  # type: ignore[arg-type]
        else:
            stream = io.TextIOWrapper(raw, encoding="utf-8")

        conversations: Dict[str, List[dict]] = defaultdict(list)
        for line in stream:
            if not line.strip():
                continue
            record = json.loads(line)
            conversation_id = str(record.get("conversation_id") or record.get("thread_id") or record.get("id"))
            conversations[conversation_id].append(record)

        raw.close()

        for conversation_id, records in conversations.items():
            records.sort(key=lambda item: item.get("created_date") or item.get("rank") or 0)
            messages: List[NormalizedMessage] = []
            for idx, record in enumerate(records):
                role = normalize_role(record.get("role") or record.get("author") or "other")
                content = record.get("text") or record.get("content") or ""
                metadata = {
                    "lang": record.get("lang", ""),
                    "parent_id": record.get("parent_id"),
                    "rank": record.get("rank"),
                }
                parent_turn = None
                if record.get("parent_id") is not None:
                    parent_turn = self._find_parent_index(records, record.get("parent_id"))
                messages.append(
                    NormalizedMessage(
                        role=role,
                        content=content,
                        turn_index=idx,
                        parent_turn_index=parent_turn,
                        metadata=metadata,
                    )
                )

            yield NormalizedConversation(
                source_id=conversation_id,
                messages=messages,
                metadata={"message_count": len(messages)},
            )

    def _find_parent_index(self, records: List[dict], parent_id: str | None) -> int | None:
        if parent_id is None:
            return None
        for index, record in enumerate(records):
            if record.get("message_id") == parent_id or record.get("id") == parent_id:
                return index
        return None

