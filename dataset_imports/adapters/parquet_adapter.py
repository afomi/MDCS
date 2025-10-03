from __future__ import annotations

from typing import Iterable, List

import pyarrow.parquet as pq

from ..models import DatasetArtifact
from ..normalization import NormalizedConversation, NormalizedMessage, normalize_role
from .base import DatasetAdapter


class ParquetConversationAdapter(DatasetAdapter):
    """Reads parquet shards containing conversation turns."""

    slug = "parquet-conversations"

    def process(self, artifact: DatasetArtifact, path: str) -> Iterable[NormalizedConversation]:
        with self.open_artifact(path, mode="rb") as stream:
            parquet_file = pq.ParquetFile(stream)
            for batch in parquet_file.iter_batches():
                rows = batch.to_pylist()
                for record in rows:
                    for conversation in self._normalize_record(record):
                        yield conversation

    def _normalize_record(self, record: dict) -> List[NormalizedConversation]:
        turns = record.get("messages") or record.get("conversation") or record.get("turns")
        if turns:
            return [self._from_turns(record, turns)]
        return []

    def _from_turns(self, record: dict, turns: Iterable[dict]) -> NormalizedConversation:
        messages: List[NormalizedMessage] = []
        for idx, turn in enumerate(turns):
            role = normalize_role(turn.get("role") or turn.get("from") or "other")
            # Some datasets use 'text' instead of 'content'/'value'
            content = turn.get("content") or turn.get("value") or turn.get("text") or ""
            metadata = {k: v for k, v in turn.items() if k not in {"role", "from", "content", "value"}}
            messages.append(
                NormalizedMessage(
                    role=role,
                    content=content,
                    turn_index=idx,
                    metadata=metadata,
                )
            )

        conversation_id = str(
            record.get("id")
            or record.get("conversation_id")
            or record.get("uuid")
            or f"record-{hash(str(record)) & 0xFFFFFFFF}"
        )
        metadata = {k: v for k, v in record.items() if k not in {"messages", "conversation", "turns"}}
        return NormalizedConversation(source_id=conversation_id, messages=messages, metadata=metadata)
