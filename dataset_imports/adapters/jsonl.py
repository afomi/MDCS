from __future__ import annotations

import io
import json
from typing import Iterable, Iterator, List

from ..models import DatasetArtifact
from ..normalization import NormalizedConversation, NormalizedMessage, normalize_role
from .base import DatasetAdapter

try:  # pragma: no cover - optional dependency
    import gzip
except ImportError:  # pragma: no cover
    gzip = None


class JSONLConversationsAdapter(DatasetAdapter):
    """Parses JSONL or JSONL.GZ files that contain serialized conversations."""

    slug = "jsonl-conversations"

    def process(self, artifact: DatasetArtifact, path: str) -> Iterable[NormalizedConversation]:
        with self.open_artifact(path, mode="rb") as raw_stream:
            if path.endswith(".gz"):
                if gzip is None:
                    raise RuntimeError("gzip module not available to read compressed artifact")
                with gzip.GzipFile(fileobj=raw_stream) as gz_stream:  # type: ignore[arg-type]
                    reader = io.TextIOWrapper(gz_stream, encoding="utf-8")
                    yield from self._iterate_lines(reader)
            else:
                reader = io.TextIOWrapper(raw_stream, encoding="utf-8")
                yield from self._iterate_lines(reader)

    def _iterate_lines(self, reader: io.TextIOBase):
        for line in reader:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            for conversation in self._normalize_record(record):
                yield conversation

    def _normalize_record(self, record: dict) -> List[NormalizedConversation]:
        if "conversations" in record or "messages" in record:
            turns = record.get("conversations") or record.get("messages")
            return [self._from_turns(record, turns)] if turns else []

        if "prompt" in record:
            return [self._from_prompt(record)]

        return []

    def _from_turns(self, record: dict, turns: Iterable[dict]) -> NormalizedConversation:
        messages: List[NormalizedMessage] = []
        for idx, turn in enumerate(turns):
            role = normalize_role(turn.get("role") or turn.get("from") or "other")
            content = turn.get("content") or turn.get("value") or ""
            messages.append(
                NormalizedMessage(
                    role=role,
                    content=content,
                    turn_index=idx,
                    metadata={k: v for k, v in turn.items() if k not in {"role", "from", "content", "value"}},
                )
            )

        conversation_id = str(
            record.get("id")
            or record.get("conversation_id")
            or record.get("thread_id")
            or record.get("uuid")
            or f"record-{hash(str(record)) & 0xFFFFFFFF}"
        )

        metadata = {k: v for k, v in record.items() if k not in {"conversations", "messages"}}
        return NormalizedConversation(source_id=conversation_id, messages=messages, metadata=metadata)

    def _from_prompt(self, record: dict) -> NormalizedConversation:
        prompt = record.get("prompt")
        if isinstance(prompt, dict):
            content = prompt.get("text") or prompt.get("content") or ""
        else:
            content = str(prompt)
        metadata = {k: v for k, v in record.items() if k != "prompt"}
        msg = NormalizedMessage(role="user", content=content, turn_index=0, metadata={})
        conversation_id = str(record.get("id") or metadata.get("prompt_id") or hash(content))
        return NormalizedConversation(source_id=conversation_id, messages=[msg], metadata=metadata)

