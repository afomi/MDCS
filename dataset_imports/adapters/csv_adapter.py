from __future__ import annotations

import csv
from collections import defaultdict
from typing import Iterable, List

from ..models import DatasetArtifact
from ..normalization import NormalizedConversation, NormalizedMessage, normalize_role
from .base import DatasetAdapter


class CSVConversationAdapter(DatasetAdapter):
    """Parses CSV exports where each row is a message belonging to a conversation."""

    slug = "csv-conversations"

    def process(self, artifact: DatasetArtifact, path: str) -> Iterable[NormalizedConversation]:
        with self.open_artifact(path, mode="r") as stream:
            reader = csv.DictReader(stream)
            buckets: dict[str, List[dict]] = defaultdict(list)
            for idx, row in enumerate(reader):
                conversation_id = (
                    row.get("conversation_id")
                    or row.get("thread_id")
                    or row.get("ticket_id")
                    or row.get("id")
                    or f"row-{idx}"
                )
                buckets[str(conversation_id)].append(row)

        for conversation_id, rows in buckets.items():
            messages: List[NormalizedMessage] = []
            for turn_index, row in enumerate(rows):
                role = normalize_role(row.get("role") or row.get("speaker") or row.get("author") or "user")
                content = row.get("content") or row.get("message") or row.get("text") or ""
                metadata = {k: v for k, v in row.items() if k not in {"role", "speaker", "author", "content", "message", "text"}}
                messages.append(
                    NormalizedMessage(
                        role=role,
                        content=content,
                        turn_index=turn_index,
                        metadata=metadata,
                    )
                )

            yield NormalizedConversation(
                source_id=str(conversation_id),
                messages=messages,
                metadata={"message_count": len(messages)},
            )

