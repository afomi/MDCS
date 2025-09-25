from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from django.core.files.base import ContentFile
from django.test import TestCase, override_settings

from dataset_imports.models import DatasetArtifact, DatasetImport
from dataset_imports.preview import collect_download_preview


def _media_root() -> str:
    return tempfile.mkdtemp()


@override_settings(MEDIA_ROOT=_media_root())
class PreviewCollectionTests(TestCase):
    def test_collects_preview_for_jsonl(self) -> None:
        dataset_import = DatasetImport.objects.create(
            slug="jsonl-dataset",
            display_name="JSONL",
            adapter="jsonl-conversations",
        )
        artifact = DatasetArtifact.objects.create(
            dataset_import=dataset_import,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        payload = {"id": "conv-1", "messages": [{"from": "human", "value": "hi"}]}
        content = json.dumps(payload) + "\n"
        artifact.file.save(
            "sample.jsonl",
            ContentFile(content.encode("utf-8")),
        )
        artifact.size_bytes = len(content)
        artifact.downloaded_bytes = len(content)
        artifact.save(update_fields=["size_bytes", "downloaded_bytes"])

        preview = collect_download_preview(artifact)
        self.assertEqual(preview.get("row_count"), 1)
        self.assertIn("conv-1", preview.get("sample_row", ""))

    def test_collects_preview_for_csv(self) -> None:
        dataset_import = DatasetImport.objects.create(
            slug="csv-dataset",
            display_name="CSV",
            adapter="csv-conversations",
        )
        artifact = DatasetArtifact.objects.create(
            dataset_import=dataset_import,
            filename="convos.csv",
            download_url="https://example.com/convos.csv",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        csv_content = "conversation_id,role,content\n1,user,Hello\n"
        artifact.file.save(
            "convos.csv",
            ContentFile(csv_content.encode("utf-8")),
        )
        artifact.size_bytes = len(csv_content)
        artifact.downloaded_bytes = len(csv_content)
        artifact.save(update_fields=["size_bytes", "downloaded_bytes"])

        preview = collect_download_preview(artifact)
        self.assertEqual(preview.get("row_count"), 1)
        self.assertIn("Hello", preview.get("sample_row", ""))

    def test_collects_preview_for_parquet(self) -> None:
        dataset_import = DatasetImport.objects.create(
            slug="parquet-dataset",
            display_name="Parquet",
            adapter="parquet-conversations",
        )
        artifact = DatasetArtifact.objects.create(
            dataset_import=dataset_import,
            filename="shard.parquet",
            download_url="https://example.com/shard.parquet",
            status=DatasetArtifact.Status.DOWNLOADED,
        )

        rows = [
            {"messages": [{"from": "human", "value": "Hey"}], "id": "row-1"}
        ]
        table = pa.Table.from_pylist(rows)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        data = buffer.getvalue().to_pybytes()

        artifact.file.save("shard.parquet", ContentFile(data))
        artifact.size_bytes = len(data)
        artifact.downloaded_bytes = len(data)
        artifact.save(update_fields=["size_bytes", "downloaded_bytes"])

        preview = collect_download_preview(artifact)
        self.assertEqual(preview.get("row_count"), 1)
        self.assertIn("row-1", preview.get("sample_row", ""))
