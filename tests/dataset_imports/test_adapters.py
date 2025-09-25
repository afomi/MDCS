from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from django.core.files.base import ContentFile
from django.test import TestCase, override_settings

from dataset_imports.adapters import get_adapter
from dataset_imports.models import (
    ConversationRecord,
    DatasetArtifact,
    DatasetImport,
    MessageRecord,
)
from dataset_imports.tasks import _normalize_artifact


def _media_root() -> str:
    return tempfile.mkdtemp()


@override_settings(MEDIA_ROOT=_media_root())
class JSONLAdapterTests(TestCase):
    def setUp(self) -> None:
        self.dataset_import = DatasetImport.objects.create(
            slug="sharegpt",
            display_name="ShareGPT",
            adapter="jsonl-conversations",
        )
        self.artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        payload = {
            "id": "conv-1",
            "conversations": [
                {"from": "human", "value": "Hello"},
                {"from": "assistant", "value": "Hi there"},
            ],
        }
        content = json.dumps(payload) + "\n"
        self.artifact.file.save(
            "sample.jsonl",
            ContentFile(content.encode("utf-8")),
        )
        self.artifact.size_bytes = len(content)
        self.artifact.downloaded_bytes = len(content)
        self.artifact.save()

    def test_jsonl_adapter_parses_conversations(self) -> None:
        adapter = get_adapter(
            self.dataset_import.adapter, self.artifact.file.field.storage
        )
        conversations = list(
            adapter.process(self.artifact, self.artifact.file.name)
        )

        self.assertEqual(len(conversations), 1)
        conversation = conversations[0]
        self.assertEqual(conversation.source_id, "conv-1")
        self.assertEqual(conversation.message_count, 2)
        self.assertEqual(conversation.messages[0].role, "user")
        self.assertEqual(conversation.messages[1].role, "assistant")

    def test_normalization_creates_asset_and_conversations(self) -> None:
        asset, stats = _normalize_artifact(self.dataset_import, self.artifact)

        self.assertGreater(stats["normalized_bytes"], 0)
        self.assertEqual(stats["conversation_count"], 1)
        self.assertEqual(stats["stored_conversations"], 1)
        self.assertEqual(asset.variant, asset.Variant.NORMALIZED_JSONL)
        self.assertEqual(asset.metadata["conversation_count"], 1)

        asset_path = Path(asset.file.path)
        self.assertTrue(asset_path.exists())
        content = asset_path.read_text(encoding="utf-8").strip()
        payload = json.loads(content)
        self.assertEqual(payload["source_id"], "conv-1")
        self.assertEqual(len(payload["messages"]), 2)

        conversations = ConversationRecord.objects.filter(dataset_import=self.dataset_import)
        self.assertEqual(conversations.count(), 1)
        messages = MessageRecord.objects.filter(conversation=conversations.first())
        self.assertEqual(messages.count(), 2)


@override_settings(MEDIA_ROOT=_media_root())
class CSVAdapterTests(TestCase):
    def setUp(self) -> None:
        self.dataset_import = DatasetImport.objects.create(
            slug="supportdesk",
            display_name="Support desk",
            adapter="csv-conversations",
        )
        self.artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="tickets.csv",
            download_url="https://example.com/tickets.csv",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        csv_content = (
            "conversation_id,role,content\n"
            "1,user,Hello support\n"
            "1,assistant,Hi!\n"
        )
        self.artifact.file.save(
            "tickets.csv",
            ContentFile(csv_content.encode("utf-8")),
        )
        self.artifact.size_bytes = len(csv_content)
        self.artifact.downloaded_bytes = len(csv_content)
        self.artifact.save()

    def test_csv_adapter_groups_rows(self) -> None:
        adapter = get_adapter(
            self.dataset_import.adapter, self.artifact.file.field.storage
        )
        conversations = list(
            adapter.process(self.artifact, self.artifact.file.name)
        )

        self.assertEqual(len(conversations), 1)
        conversation = conversations[0]
        self.assertEqual(conversation.source_id, "1")
        self.assertEqual(conversation.message_count, 2)
        self.assertEqual(conversation.messages[0].content, "Hello support")
        self.assertEqual(conversation.messages[1].role, "assistant")


@override_settings(MEDIA_ROOT=_media_root())
class ParquetAdapterTests(TestCase):
    def setUp(self) -> None:
        self.dataset_import = DatasetImport.objects.create(
            slug="wildchat",
            display_name="WildChat",
            adapter="parquet-conversations",
        )
        self.artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="shard.parquet",
            download_url="https://example.com/shard.parquet",
            status=DatasetArtifact.Status.DOWNLOADED,
        )

        messages = [
            {
                "messages": [
                    {"from": "human", "value": "Hello"},
                    {"from": "assistant", "value": "Hi!"},
                ],
                "id": "wc-1",
                "source": "synthetic",
            }
        ]
        table = pa.Table.from_pylist(messages)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        data = buffer.getvalue().to_pybytes()

        self.artifact.file.save("shard.parquet", ContentFile(data))
        self.artifact.size_bytes = len(data)
        self.artifact.downloaded_bytes = len(data)
        self.artifact.save()

    def test_parquet_adapter_reads_rows(self) -> None:
        adapter = get_adapter(
            self.dataset_import.adapter, self.artifact.file.field.storage
        )
        conversations = list(
            adapter.process(self.artifact, self.artifact.file.name)
        )

        self.assertEqual(len(conversations), 1)
        conversation = conversations[0]
        self.assertEqual(conversation.source_id, "wc-1")
        self.assertEqual(conversation.message_count, 2)
        self.assertEqual(conversation.messages[1].content, "Hi!")


@override_settings(MEDIA_ROOT=_media_root())
class OASSTAdapterTests(TestCase):
    def setUp(self) -> None:
        self.dataset_import = DatasetImport.objects.create(
            slug="oasst",
            display_name="OpenAssistant",
            adapter="oasst-messages",
        )
        self.artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="messages.jsonl",
            download_url="https://example.com/messages.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        lines = [
            json.dumps(
                {
                    "conversation_id": "tree-1",
                    "message_id": "msg-1",
                    "role": "user",
                    "text": "Hi",
                    "lang": "en",
                }
            ),
            json.dumps(
                {
                    "conversation_id": "tree-1",
                    "message_id": "msg-2",
                    "parent_id": "msg-1",
                    "role": "assistant",
                    "text": "Hello!",
                    "lang": "en",
                }
            ),
        ]
        content = "\n".join(lines) + "\n"
        self.artifact.file.save(
            "messages.jsonl",
            ContentFile(content.encode("utf-8")),
        )
        self.artifact.size_bytes = len(content)
        self.artifact.downloaded_bytes = len(content)
        self.artifact.save()

    def test_oasst_adapter_groups_conversation(self) -> None:
        adapter = get_adapter(
            self.dataset_import.adapter, self.artifact.file.field.storage
        )
        conversations = list(
            adapter.process(self.artifact, self.artifact.file.name)
        )

        self.assertEqual(len(conversations), 1)
        conversation = conversations[0]
        self.assertEqual(conversation.source_id, "tree-1")
        self.assertEqual(conversation.message_count, 2)
        self.assertEqual(conversation.messages[1].parent_turn_index, 0)

