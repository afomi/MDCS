from __future__ import annotations

import json
import tempfile
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.core.files.base import ContentFile
from django.test import override_settings

from dataset_imports.catalog import ArtifactDefinition, DatasetDefinition
from dataset_imports.models import (
    AnalysisJob,
    DatasetArtifact,
    DatasetAsset,
    DatasetImport,
    DatasetImportEvent,
    ConversationRecord,
)


class DatasetDetailStatusTests(TestCase):
    @patch("dataset_imports.views.get_dataset")
    def test_detail_reconciles_download_status(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="queued-import",
            display_name="Queued Import",
            adapter="unit-test",
            status=DatasetImport.Status.QUEUED,
            notes={
                "dataset_id": dataset_id,
            },
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )

        response = self.client.get(reverse("dataset_imports:detail", args=[dataset_id]))
        self.assertEqual(response.status_code, 200)

        import_obj.refresh_from_db()
        self.assertEqual(import_obj.status, DatasetImport.Status.DOWNLOADED)

    @patch("dataset_imports.views.get_dataset")
    def test_completed_dataset_hides_start_import(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[],
        )
        mock_get_dataset.return_value = dataset_def

        DatasetImport.objects.create(
            slug="completed-import",
            display_name="Completed Import",
            adapter="unit-test",
            status=DatasetImport.Status.COMPLETED,
            notes={
                "dataset_id": dataset_id,
            },
        )

        response = self.client.get(reverse("dataset_imports:detail", args=[dataset_id]))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["dataset_completed"])
        self.assertNotContains(response, "Start Import")
        self.assertContains(response, "Reset Import")

    @patch("dataset_imports.views.get_dataset")
    def test_pipeline_steps_show_transformation_progress(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl.gz",
                    download_url="https://example.com/sample.jsonl.gz",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="pipeline-import",
            display_name="Pipeline Import",
            adapter="unit-test",
            status=DatasetImport.Status.COMPLETED,
            analysis_status="completed",
            notes={
                "dataset_id": dataset_id,
                "analysis_mode": "top-down",
                "analysis_summary": {"artifact_count": 1},
            },
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl.gz",
            download_url="https://example.com/sample.jsonl.gz",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        with override_settings(MEDIA_ROOT=tempfile.mkdtemp()):
            DatasetAsset.objects.create(
                dataset_import=import_obj,
                variant=DatasetAsset.Variant.RAW,
                description="Extracted JSON",
                file=ContentFile(b"{}", name="sample.jsonl"),
            )
        ConversationRecord.objects.create(
            dataset_import=import_obj,
            source_id="conversation-1",
        )

        response = self.client.get(reverse("dataset_imports:detail", args=[dataset_id]))

        self.assertEqual(response.status_code, 200)
        pipeline_steps = response.context["pipeline_steps"]
        self.assertEqual(len(pipeline_steps), 4)
        statuses = {step["key"]: step["status"] for step in pipeline_steps}
        self.assertEqual(statuses.get("acquire"), "completed")
        self.assertEqual(statuses.get("extract"), "completed")
        self.assertEqual(statuses.get("normalize"), "completed")
        self.assertEqual(statuses.get("analyze"), "completed")
        self.assertEqual(response.context["pipeline_current_step"], 4)
        self.assertEqual(response.context["pipeline_current_title"], "Analyze")

    @patch("dataset_imports.views.get_dataset")
    def test_preview_toggle_hidden_by_default(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="preview-import",
            display_name="Preview Import",
            adapter="unit-test",
            status=DatasetImport.Status.DOWNLOADED,
            notes={"dataset_id": dataset_id},
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=1,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
            metadata={
                "preview": {
                    "row_count": 5,
                    "sample_row": "{\"foo\": \"bar\"}",
                }
            },
        )

        response = self.client.get(reverse("dataset_imports:detail", args=[dataset_id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Preview record")
        self.assertContains(response, 'data-role="preview-wrapper"')
        self.assertContains(response, 'aria-hidden="true"')

    @patch("dataset_imports.views.get_dataset")
    def test_start_analysis_returns_message_when_already_running(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="analyzing-import",
            display_name="Analyzing Import",
            adapter="unit-test",
            status=DatasetImport.Status.ANALYZING,
            notes={
                "dataset_id": dataset_id,
            },
        )

        url = reverse("dataset_imports:analyze", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get("message"), "Analysis already running")

    @patch("dataset_imports.views.run_dataset_analysis.delay")
    @patch("dataset_imports.views.get_dataset")
    def test_start_analysis_records_task_id(self, mock_get_dataset, mock_delay) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[],
        )
        mock_get_dataset.return_value = dataset_def

        async_result = MagicMock()
        async_result.id = "task-123"
        mock_delay.return_value = async_result

        import_obj = DatasetImport.objects.create(
            slug="ready-import",
            display_name="Ready Import",
            adapter="unit-test",
            status=DatasetImport.Status.DOWNLOADED,
            notes={"dataset_id": dataset_id},
        )

        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.COMPLETED,
            processed_bytes=2048,
        )

        url = reverse("dataset_imports:analyze", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 200)
        import_obj.refresh_from_db()
        self.assertEqual(import_obj.notes.get("analysis_task_id"), "task-123")

        job = AnalysisJob.objects.get(dataset_import=import_obj)
        self.assertEqual(job.status, AnalysisJob.Status.RUNNING)
        self.assertEqual(job.task_id, "task-123")
        mock_delay.assert_called_once()
        args, kwargs = mock_delay.call_args
        self.assertEqual(args[0], import_obj.pk)
        self.assertEqual(kwargs.get("job_id"), job.pk)

    @patch("dataset_imports.views.run_dataset_analysis.delay")
    @patch("dataset_imports.views.get_dataset")
    def test_start_analysis_requires_normalized_artifacts(self, mock_get_dataset, mock_delay) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="downloaded-import",
            display_name="Downloaded Import",
            adapter="unit-test",
            status=DatasetImport.Status.DOWNLOADED,
            notes={"dataset_id": dataset_id},
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
            downloaded_bytes=1024,
        )

        url = reverse("dataset_imports:analyze", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertIn("error", payload)
        mock_delay.assert_not_called()

    @patch("dataset_imports.views.get_dataset")
    def test_start_normalization_waits_for_downloads(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="waiting-import",
            display_name="Waiting Import",
            adapter="unit-test",
            status=DatasetImport.Status.DOWNLOADED,
            notes={"dataset_id": dataset_id},
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADING,
        )

        url = reverse("dataset_imports:normalize", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("Waiting for 1 artifact", payload.get("message"))

    @patch("dataset_imports.views.run_dataset_analysis.delay")
    @patch("dataset_imports.views.get_dataset")
    def test_start_analysis_allows_completed_status(self, mock_get_dataset, mock_delay) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        async_result = MagicMock()
        async_result.id = "task-456"
        mock_delay.return_value = async_result

        import_obj = DatasetImport.objects.create(
            slug="completed-import",
            display_name="Completed Import",
            adapter="unit-test",
            status=DatasetImport.Status.COMPLETED,
            analysis_status="completed",
            notes={"dataset_id": dataset_id},
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.COMPLETED,
            downloaded_bytes=1024,
            processed_bytes=1024,
        )

        url = reverse("dataset_imports:analyze", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 200)
        import_obj.refresh_from_db()
        self.assertEqual(import_obj.notes.get("analysis_task_id"), "task-456")

        job = AnalysisJob.objects.get(dataset_import=import_obj)
        mock_delay.assert_called_once()
        _, kwargs = mock_delay.call_args
        self.assertEqual(kwargs.get("job_id"), job.pk)

    @patch("dataset_imports.views.get_dataset")
    def test_status_api_marks_completed_import_as_analyzable(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="completed-import",
            display_name="Completed Import",
            adapter="unit-test",
            status=DatasetImport.Status.COMPLETED,
            analysis_status="completed",
            notes={"dataset_id": dataset_id},
            downloaded_bytes=1024,
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.COMPLETED,
            downloaded_bytes=1024,
            processed_bytes=1024,
        )

        url = reverse("dataset_imports:status", args=[dataset_id])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload.get("can_analyze"))

    @patch("dataset_imports.views.get_dataset")
    def test_status_api_includes_selected_artifacts_metadata(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset-selected"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Selectable Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="file-a.jsonl",
                    download_url="https://example.com/file-a.jsonl",
                ),
                ArtifactDefinition(
                    filename="file-b.jsonl",
                    download_url="https://example.com/file-b.jsonl",
                ),
            ],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="selected-import",
            display_name="Selected Import",
            adapter="unit-test",
            status=DatasetImport.Status.DOWNLOADED,
            notes={
                "dataset_id": dataset_id,
                "selected_artifacts": ["file-a.jsonl"],
            },
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="file-a.jsonl",
            download_url="https://example.com/file-a.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=1,
            filename="file-b.jsonl",
            download_url="https://example.com/file-b.jsonl",
            status=DatasetArtifact.Status.PENDING,
        )

        url = reverse("dataset_imports:status", args=[dataset_id])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get("selected_artifacts"), ["file-a.jsonl"])
        self.assertFalse(payload.get("can_launch"))

    @patch("dataset_imports.views.get_dataset")
    def test_status_api_defaults_selection_when_no_import(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset-empty"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Selectable Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="file-a.jsonl",
                    download_url="https://example.com/file-a.jsonl",
                ),
                ArtifactDefinition(
                    filename="file-b.jsonl",
                    download_url="https://example.com/file-b.jsonl",
                ),
            ],
        )
        mock_get_dataset.return_value = dataset_def

        url = reverse("dataset_imports:status", args=[dataset_id])
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload.get("selected_artifacts"),
            ["file-a.jsonl", "file-b.jsonl"],
        )
        self.assertTrue(payload.get("can_launch"))

    @patch("dataset_imports.views.run_dataset_import.delay")
    @patch("dataset_imports.views.get_dataset")
    def test_detail_start_import_honors_selected_artifacts(self, mock_get_dataset, mock_delay) -> None:
        dataset_id = "start-import-selection"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Selectable Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="file-a.jsonl",
                    download_url="https://example.com/file-a.jsonl",
                ),
                ArtifactDefinition(
                    filename="file-b.jsonl",
                    download_url="https://example.com/file-b.jsonl",
                ),
            ],
        )
        mock_get_dataset.return_value = dataset_def
        mock_delay.return_value = None

        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="staff",
            password="password",
            is_staff=True,
        )
        self.client.force_login(user)

        response = self.client.post(
            reverse("dataset_imports:detail", args=[dataset_id]),
            {
                "action": "start-import",
                "selected_artifacts": ["file-a.jsonl"],
                "artifact_limit": "2",
                "cluster_count": "5",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        import_obj = DatasetImport.objects.latest("created_at")
        self.assertEqual(import_obj.notes.get("selected_artifacts"), ["file-a.jsonl"])
        self.assertEqual(import_obj.notes.get("artifact_limit"), 1)
        self.assertEqual(import_obj.total_artifacts, 1)
        self.assertEqual(import_obj.artifacts.count(), 1)
        self.assertEqual(import_obj.artifacts.first().filename, "file-a.jsonl")
        mock_delay.assert_called_once_with(import_obj.pk)

    @patch("dataset_imports.views.process_artifact.delay")
    @patch("dataset_imports.views.get_dataset")
    def test_start_normalization_enqueues_downloaded_artifacts(self, mock_get_dataset, mock_delay) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[
                ArtifactDefinition(
                    filename="sample.jsonl",
                    download_url="https://example.com/sample.jsonl",
                )
            ],
        )
        mock_get_dataset.return_value = dataset_def
        mock_delay.side_effect = lambda artifact_id: None

        import_obj = DatasetImport.objects.create(
            slug="downloaded-import",
            display_name="Downloaded Import",
            adapter="unit-test",
            status=DatasetImport.Status.DOWNLOADED,
            notes={"dataset_id": dataset_id},
        )
        artifact = DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.DOWNLOADED,
            downloaded_bytes=2048,
        )

        url = reverse("dataset_imports:normalize", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get('message'), 'Queued normalization for 1 artifact.')
        import_obj.refresh_from_db()
        self.assertEqual(import_obj.status, DatasetImport.Status.PROCESSING)
        mock_delay.assert_called_once_with(artifact.pk)

    @patch("dataset_imports.views.get_dataset")
    def test_start_normalization_returns_message_when_nothing_to_do(self, mock_get_dataset) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[],
        )
        mock_get_dataset.return_value = dataset_def

        import_obj = DatasetImport.objects.create(
            slug="processed-import",
            display_name="Processed Import",
            adapter="unit-test",
            status=DatasetImport.Status.COMPLETED,
            notes={"dataset_id": dataset_id},
        )
        DatasetArtifact.objects.create(
            dataset_import=import_obj,
            sequence=0,
            filename="sample.jsonl",
            download_url="https://example.com/sample.jsonl",
            status=DatasetArtifact.Status.COMPLETED,
            processed_bytes=2048,
        )

        url = reverse("dataset_imports:normalize", args=[dataset_id])
        response = self.client.post(url, content_type="application/json")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload.get('message'), 'No downloaded artifacts awaiting normalization.')

    @patch("dataset_imports.views.celery_app")
    @patch("dataset_imports.views.get_dataset")
    def test_cancel_analysis_revokes_task(self, mock_get_dataset, mock_celery_app) -> None:
        dataset_id = "test-dataset"
        dataset_def = DatasetDefinition(
            id=dataset_id,
            name="Test Dataset",
            description="",
            adapter="unit-test",
            artifacts=[],
        )
        mock_get_dataset.return_value = dataset_def
        mock_celery_app.control.revoke.return_value = None

        import_obj = DatasetImport.objects.create(
            slug="analyzing-import",
            display_name="Analyzing Import",
            adapter="unit-test",
            status=DatasetImport.Status.ANALYZING,
            analysis_status="running",
            notes={
                "dataset_id": dataset_id,
                "analysis_task_id": "task-123",
            },
        )

        url = reverse("dataset_imports:analysis-cancel", args=[dataset_id])
        response = self.client.post(
            url,
            data=json.dumps({"reason": "Testing"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        mock_celery_app.control.revoke.assert_called_once()
        revoke_args, revoke_kwargs = mock_celery_app.control.revoke.call_args
        self.assertEqual(revoke_args[0], "task-123")
        self.assertTrue(revoke_kwargs.get("terminate"))

        import_obj.refresh_from_db()
        self.assertEqual(import_obj.status, DatasetImport.Status.DOWNLOADED)
        self.assertEqual(import_obj.analysis_status, "cancelled")
        self.assertNotIn("analysis_task_id", import_obj.notes)
        self.assertTrue(
            import_obj.events.filter(
                event_type=DatasetImportEvent.Type.ANALYSIS_CANCEL
            ).exists()
        )
