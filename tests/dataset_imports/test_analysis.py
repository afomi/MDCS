from __future__ import annotations

import json
import tempfile

from django.core.files.base import ContentFile
from django.test import TestCase, override_settings

from dataset_imports.models import AnalysisJob, AnalysisResult, DatasetArtifact, DatasetImport
from dataset_imports.tasks import (
    ANALYSIS_ALGORITHM_DEFAULT,
    _normalize_artifact,
    run_dataset_analysis,
)


def _media_root() -> str:
    return tempfile.mkdtemp()


@override_settings(MEDIA_ROOT=_media_root())
class AnalysisSummaryTests(TestCase):
    def setUp(self) -> None:
        self.dataset_import = DatasetImport.objects.create(
            slug="analysis-dataset",
            display_name="Analysis Dataset",
            adapter="jsonl-conversations",
            status=DatasetImport.Status.DOWNLOADED,
        )
        self.artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )

        payload = {
            "id": "conversation-1",
            "conversations": [
                {"from": "human", "value": "Hello there"},
                {"from": "assistant", "value": "Hi friend"},
            ],
        }
        encoded = (json.dumps(payload) + "\n").encode("utf-8")
        self.artifact.file.save("sample.jsonl", ContentFile(encoded))
        self.artifact.size_bytes = len(encoded)
        self.artifact.downloaded_bytes = len(encoded)
        self.artifact.metadata = {"preview": {"row_count": 1}}
        self.artifact.save()

        _normalize_artifact(self.dataset_import, self.artifact)
        self.artifact.refresh_from_db()
        self.artifact.status = DatasetArtifact.Status.COMPLETED
        self.artifact.save(update_fields=["status"])

    def test_analysis_task_records_overview_metrics(self) -> None:
        job = AnalysisJob.objects.create(
            dataset_import=self.dataset_import,
            requested_mode="top-down",
        )

        run_dataset_analysis.__wrapped__(run_dataset_analysis, self.dataset_import.pk, job.pk)

        self.dataset_import.refresh_from_db()
        summary = self.dataset_import.notes.get("analysis_summary")

        self.assertIsNotNone(summary)
        self.assertEqual(summary.get("artifact_count"), 1)

        artifacts = summary.get("artifacts") or []
        self.assertEqual(len(artifacts), 1)
        artifact_summary = artifacts[0]
        self.assertEqual(artifact_summary.get("row_count"), 1)
        self.assertGreater(artifact_summary.get("word_count", 0), 0)

        self.assertEqual(summary.get("total_row_count"), 1)
        self.assertGreater(summary.get("total_word_count", 0), 0)

        self.assertEqual(
            self.dataset_import.status,
            DatasetImport.Status.COMPLETED,
        )

        result = AnalysisResult.objects.get(dataset_import=self.dataset_import)
        self.assertEqual(result.summary.get("artifact_count"), 1)
        self.assertEqual(result.dataset_id, self.dataset_import.adapter)
        self.assertEqual(result.algorithm, ANALYSIS_ALGORITHM_DEFAULT)

        job.refresh_from_db()
        self.assertEqual(job.status, AnalysisJob.Status.COMPLETED)
        self.assertIsNotNone(job.finished_at)
        self.assertEqual(job.algorithm, ANALYSIS_ALGORITHM_DEFAULT)

    def test_topic_cluster_analysis_generates_clusters(self) -> None:
        second_artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="sample-2.jsonl",
            download_url="https://example.com/sample2.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )

        payload = {
            "id": "conversation-2",
            "conversations": [
                {"from": "user", "value": "Tell me about diffusion in materials"},
                {"from": "assistant", "value": "Diffusion describes how particles move from high concentration to low concentration."},
            ],
        }
        encoded = (json.dumps(payload) + "\n").encode("utf-8")
        second_artifact.file.save("sample-2.jsonl", ContentFile(encoded))
        second_artifact.size_bytes = len(encoded)
        second_artifact.downloaded_bytes = len(encoded)
        second_artifact.metadata = {}
        second_artifact.save()

        _normalize_artifact(self.dataset_import, second_artifact)
        second_artifact.refresh_from_db()
        second_artifact.status = DatasetArtifact.Status.COMPLETED
        second_artifact.save(update_fields=["status"])

        self.dataset_import.adapter = "parquet-conversations"
        self.dataset_import.notes = {
            "dataset_id": "lmsys_chat_1m",
            "analysis_mode": "topic-clusters",
            "analysis_cluster_count": 2,
        }
        self.dataset_import.save(update_fields=["adapter", "notes"])

        job = AnalysisJob.objects.create(
            dataset_import=self.dataset_import,
            requested_mode="topic-clusters",
            requested_cluster_count=2,
        )

        run_dataset_analysis.__wrapped__(run_dataset_analysis, self.dataset_import.pk, job.pk)

        self.dataset_import.refresh_from_db()
        summary = self.dataset_import.notes.get("analysis_summary") or {}
        clusters = summary.get("clusters") or []

        self.assertTrue(clusters)
        first_cluster = clusters[0]
        self.assertIn("top_terms", first_cluster)
        self.assertGreaterEqual(first_cluster.get("size", 0), 1)
        overview = summary.get("cluster_overview") or {}
        self.assertGreater(overview.get("cluster_count", 0), 0)
        self.assertEqual(overview.get("algorithm"), "minibatch-kmeans")
        self.assertEqual(summary.get("requested_cluster_count"), 2)
        self.assertEqual(overview.get("requested_count"), 2)
        self.assertEqual(overview.get("status"), "ok")

        job.refresh_from_db()
        self.assertEqual(job.status, AnalysisJob.Status.COMPLETED)
        self.assertEqual(job.requested_cluster_count, 2)
        self.assertGreaterEqual(job.cluster_count or 0, 1)
        self.assertEqual(job.metadata.get("summary_status"), "ok")

    def test_topic_cluster_handles_insufficient_conversations(self) -> None:
        self.dataset_import.adapter = "parquet-conversations"
        self.dataset_import.notes = {
            "dataset_id": "lmsys_chat_1m",
            "analysis_mode": "topic-clusters",
            "analysis_cluster_count": 5,
        }
        self.dataset_import.save(update_fields=["adapter", "notes"])

        job = AnalysisJob.objects.create(
            dataset_import=self.dataset_import,
            requested_mode="topic-clusters",
            requested_cluster_count=5,
        )

        run_dataset_analysis.__wrapped__(run_dataset_analysis, self.dataset_import.pk, job.pk)

        self.dataset_import.refresh_from_db()
        summary = self.dataset_import.notes.get("analysis_summary") or {}
        overview = summary.get("cluster_overview") or {}

        self.assertEqual(summary.get("requested_cluster_count"), 5)
        self.assertEqual(overview.get("requested_count"), 5)
        self.assertEqual(overview.get("status"), "insufficient-documents")
        self.assertEqual(summary.get("clusters"), [])
        self.assertIn("required", (overview.get("note") or "").lower())

        job.refresh_from_db()
        self.assertEqual(job.status, AnalysisJob.Status.COMPLETED)
        self.assertEqual(job.cluster_count or 0, 0)
        self.assertEqual(job.metadata.get("cluster_overview", {}).get("status"), "insufficient-documents")

    def test_topic_cluster_handles_empty_groups(self) -> None:
        second_artifact = DatasetArtifact.objects.create(
            dataset_import=self.dataset_import,
            filename="sample-2.jsonl",
            download_url="https://example.com/sample2.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )

        payload = {
            "id": "conversation-2",
            "conversations": [
                {"from": "user", "value": "Single topic conversation"},
            ],
        }
        encoded = (json.dumps(payload) + "\n").encode("utf-8")
        second_artifact.file.save("sample-2.jsonl", ContentFile(encoded))
        second_artifact.size_bytes = len(encoded)
        second_artifact.downloaded_bytes = len(encoded)
        second_artifact.metadata = {}
        second_artifact.save()

        _normalize_artifact(self.dataset_import, second_artifact)
        second_artifact.refresh_from_db()
        second_artifact.status = DatasetArtifact.Status.COMPLETED
        second_artifact.save(update_fields=["status"])

        self.dataset_import.adapter = "parquet-conversations"
        self.dataset_import.notes = {
            "dataset_id": "lmsys_chat_1m",
            "analysis_mode": "topic-clusters",
            "analysis_cluster_count": 4,
        }
        self.dataset_import.save(update_fields=["adapter", "notes"])

        job = AnalysisJob.objects.create(
            dataset_import=self.dataset_import,
            requested_mode="topic-clusters",
            requested_cluster_count=4,
        )

        run_dataset_analysis.__wrapped__(run_dataset_analysis, self.dataset_import.pk, job.pk)

        self.dataset_import.refresh_from_db()
        summary = self.dataset_import.notes.get("analysis_summary") or {}
        overview = summary.get("cluster_overview") or {}

        self.assertEqual(summary.get("requested_cluster_count"), 4)
        self.assertEqual(overview.get("requested_count"), 4)
        self.assertIn(overview.get("status"), {"ok", "no-clusters"})

        job.refresh_from_db()
        self.assertEqual(job.status, AnalysisJob.Status.COMPLETED)
        self.assertEqual(job.requested_cluster_count, 4)
