from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, asdict
from typing import Dict, Iterable, List, Optional


ROLE_MAP = {
    "human": "user",
    "user": "user",
    "assistant": "assistant",
    "bot": "assistant",
    "system": "system",
    "tool": "tool",
    "moderator": "moderator",
}


def normalize_role(value: str) -> str:
    return ROLE_MAP.get(value.lower(), "other")


@dataclass
class NormalizedMessage:
    role: str
    content: str
    turn_index: int
    parent_turn_index: Optional[int] = None
    metadata: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class NormalizedConversation:
    source_id: str
    messages: List[NormalizedMessage]
    metadata: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        data["messages"] = [message.to_dict() for message in self.messages]
        return data

    @property
    def message_count(self) -> int:
        return len(self.messages)

    def checksum(self) -> str:
        payload = "\n".join(
            f"{msg.role}:{msg.turn_index}:{msg.parent_turn_index}:{msg.content}"
            for msg in self.messages
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def conversations_to_jsonl(conversations: Iterable[NormalizedConversation]) -> Iterable[str]:
    import json

    for conversation in conversations:
        yield json.dumps(conversation.to_dict(), ensure_ascii=False)




from django.db import transaction

from .models import ConversationRecord, MessageRecord

DB_BATCH_SIZE = 500


def load_conversations(dataset_import, conversations):
    """Persist normalized conversations/messages to the database."""

    conversations = list(conversations)
    if not conversations:
        return 0

    created = 0
    with transaction.atomic():
        for chunk in _chunk(conversations, DB_BATCH_SIZE):
            created += _load_chunk(dataset_import, chunk)
    return created


def _chunk(items, size):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _load_chunk(dataset_import, conversations):
    created = 0
    for convo in conversations:
        conv_obj, was_created = ConversationRecord.objects.get_or_create(
            dataset_import=dataset_import,
            source_id=convo.source_id,
            defaults={
                'title': convo.metadata.get('title', ''),
                'metadata': convo.metadata,
            },
        )
        if not was_created:
            continue

        messages = []
        for message in convo.messages:
            messages.append(
                MessageRecord(
                    conversation=conv_obj,
                    turn_index=message.turn_index,
                    parent_turn_index=message.parent_turn_index,
                    role=message.role,
                    content=message.content,
                    lang=message.metadata.get('lang', ''),
                    content_hash=_hash_text(message.content),
                    metadata=message.metadata,
                )
            )
        MessageRecord.objects.bulk_create(messages)
        created += 1
    return created


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()
