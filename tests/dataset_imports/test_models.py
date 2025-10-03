from __future__ import annotations

from django.test import TestCase

from dataset_imports.models import (
    DatasetArtifact,
    DatasetImport,
    reconcile_import_status,
    sync_import_aggregates,
)


class ImportStatusReconciliationTests(TestCase):
    def _build_import(self, *, artifact_statuses: list[str]) -> DatasetImport:
        dataset_import = DatasetImport.objects.create(
            slug="test-import",
            display_name="Test Import",
            adapter="unit-test-adapter",
        )

        for index, status in enumerate(artifact_statuses):
            DatasetArtifact.objects.create(
                dataset_import=dataset_import,
                sequence=index,
                filename=f"artifact-{index}.jsonl",
                download_url="https://example.com/data.jsonl",
                status=status,
            )

        dataset_import.status = DatasetImport.Status.QUEUED
        dataset_import.save(update_fields=["status"])
        return dataset_import

    def test_marks_import_downloaded_when_all_artifacts_ready(self) -> None:
        import_obj = self._build_import(
            artifact_statuses=[
                DatasetArtifact.Status.DOWNLOADED,
                DatasetArtifact.Status.DOWNLOADED,
            ]
        )

        reconcile_import_status(import_obj)
        import_obj.refresh_from_db()

        self.assertEqual(import_obj.status, DatasetImport.Status.DOWNLOADED)

    def test_does_not_change_status_while_downloads_pending(self) -> None:
        import_obj = self._build_import(
            artifact_statuses=[
                DatasetArtifact.Status.DOWNLOADING,
                DatasetArtifact.Status.DOWNLOADED,
            ]
        )

        reconcile_import_status(import_obj)
        import_obj.refresh_from_db()

        self.assertEqual(import_obj.status, DatasetImport.Status.QUEUED)

    def test_marks_import_failed_when_any_download_fails(self) -> None:
        import_obj = self._build_import(
            artifact_statuses=[
                DatasetArtifact.Status.DOWNLOADED,
                DatasetArtifact.Status.FAILED,
            ]
        )

        reconcile_import_status(import_obj)
        import_obj.refresh_from_db()

        self.assertEqual(import_obj.status, DatasetImport.Status.FAILED)
        self.assertIsNotNone(import_obj.finished_at)

    def test_sync_import_aggregates_invokes_status_reconciliation(self) -> None:
        import_obj = self._build_import(
            artifact_statuses=[
                DatasetArtifact.Status.DOWNLOADED,
            ]
        )

        sync_import_aggregates(import_obj)
        import_obj.refresh_from_db()

        self.assertEqual(import_obj.status, DatasetImport.Status.DOWNLOADED)

    def test_reset_analysis_returns_import_to_downloaded_state(self) -> None:
        import_obj = self._build_import(
            artifact_statuses=[
                DatasetArtifact.Status.DOWNLOADED,
            ]
        )

        import_obj.status = DatasetImport.Status.ANALYZING
        import_obj.analysis_status = "running"
        import_obj.analysis_progress = 42
        import_obj.analysis_message = "Halfway there"
        import_obj.analysis_started_at = import_obj.analysis_finished_at = None
        import_obj.save(
            update_fields=[
                "status",
                "analysis_status",
                "analysis_progress",
                "analysis_message",
            ]
        )

        import_obj.reset_analysis()
        import_obj.refresh_from_db()

        self.assertEqual(import_obj.status, DatasetImport.Status.DOWNLOADED)
        self.assertEqual(import_obj.analysis_status, "")
        self.assertEqual(import_obj.analysis_progress, 0)
        self.assertEqual(import_obj.analysis_message, "")

    def test_cancel_analysis_sets_status_and_message(self) -> None:
        import_obj = self._build_import(
            artifact_statuses=[
                DatasetArtifact.Status.DOWNLOADED,
            ]
        )

        import_obj.status = DatasetImport.Status.ANALYZING
        import_obj.analysis_status = "running"
        import_obj.analysis_progress = 45
        import_obj.analysis_message = "Working"
        import_obj.notes = {"analysis_task_id": "task-1"}
        import_obj.save(
            update_fields=[
                "status",
                "analysis_status",
                "analysis_progress",
                "analysis_message",
                "notes",
            ]
        )

        import_obj.cancel_analysis(reason="User requested cancel")
        import_obj.refresh_from_db()

        self.assertEqual(import_obj.status, DatasetImport.Status.DOWNLOADED)
        self.assertEqual(import_obj.analysis_status, "cancelled")
        self.assertEqual(import_obj.analysis_progress, 0)
        self.assertEqual(import_obj.analysis_message, "User requested cancel")
        self.assertNotIn("analysis_task_id", import_obj.notes)
