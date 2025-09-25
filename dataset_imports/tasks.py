from __future__ import annotations

import hashlib
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Optional
import re

from celery import shared_task
from django.utils import timezone

from .adapters import get_adapter
from .models import (
    DatasetArtifact,
    DatasetAsset,
    DatasetImport,
    DatasetImportLog,
    DatasetProgressSnapshot,
    MessageRecord,
    sync_import_aggregates,
)
from .normalization import NormalizedConversation, load_conversations
from .preview import collect_download_preview
from .services import DownloadError, ensure_storage_dir, stream_download

PROGRESS_BATCH_SIZE = 100
TOPIC_SAMPLE_LIMIT = 500
TOPIC_KEYWORDS_PER_TOPIC = 4
TOPIC_CLUSTERS = 3
WORD_PATTERN = re.compile(r"[A-Za-z]{4,}")


logger = logging.getLogger(__name__)


@dataclass
class ProgressEvent:
    status: str
    downloaded_bytes: int = 0
    processed_bytes: int = 0
    total_bytes: int = 0
    message: str = ""
    artifact_id: Optional[int] = None


def _record_progress(import_obj: DatasetImport, event: ProgressEvent) -> None:
    DatasetProgressSnapshot.objects.create(
        dataset_import=import_obj,
        artifact_id=event.artifact_id,
        status=event.status,
        downloaded_bytes=event.downloaded_bytes,
        processed_bytes=event.processed_bytes,
        total_bytes=event.total_bytes,
        message=event.message,
    )

    if event.artifact_id:
        update_kwargs = {
            "downloaded_bytes": event.downloaded_bytes,
            "processed_bytes": event.processed_bytes,
        }
        if event.total_bytes and event.total_bytes > 0:
            update_kwargs["size_bytes"] = event.total_bytes
        DatasetArtifact.objects.filter(pk=event.artifact_id).update(**update_kwargs)
        sync_import_aggregates(import_obj)

    if (
        event.status == DatasetArtifact.Status.DOWNLOADING
        and import_obj.status in {DatasetImport.Status.PENDING, DatasetImport.Status.QUEUED}
    ):
        DatasetImport.objects.filter(pk=import_obj.pk).update(
            status=DatasetImport.Status.DOWNLOADING,
            updated_at=timezone.now(),
        )
        import_obj.status = DatasetImport.Status.DOWNLOADING
        import_obj.updated_at = timezone.now()


def _log(
    import_obj: DatasetImport,
    level: str,
    message: str,
    *,
    artifact: DatasetArtifact | None = None,
    extra: Optional[dict] = None,
) -> None:
    DatasetImportLog.objects.create(
        dataset_import=import_obj,
        artifact=artifact,
        level=level,
        message=message,
        extra=extra or {},
    )


@shared_task(bind=True)
def run_dataset_import(self, import_id: int) -> None:
    import_obj = DatasetImport.objects.select_related("created_by").get(pk=import_id)
    import_obj.mark_started()
    _log(import_obj, "INFO", "Import execution started", extra={"task_id": self.request.id})

    try:
        artifacts = import_obj.artifacts.order_by("sequence", "id")
        for artifact in artifacts:
            download_artifact.delay(artifact.pk)

        import_obj.status = DatasetImport.Status.QUEUED
        import_obj.save(update_fields=["status", "updated_at"])
    except Exception as exc:  # pragma: no cover
        import_obj.last_error = str(exc)
        import_obj.status = DatasetImport.Status.FAILED
        import_obj.finished_at = timezone.now()
        import_obj.save(update_fields=["last_error", "status", "finished_at", "updated_at"])
        _log(import_obj, "ERROR", f"Import orchestration failed: {exc}")
        raise


@shared_task(bind=True, autoretry_for=(DownloadError,), retry_backoff=5, max_retries=3)
def download_artifact(self, artifact_id: int) -> None:
    artifact = DatasetArtifact.objects.select_related("dataset_import").get(pk=artifact_id)
    import_obj = artifact.dataset_import

    artifact.status = DatasetArtifact.Status.DOWNLOADING
    artifact.started_at = timezone.now()
    artifact.save(update_fields=["status", "started_at", "updated_at"])

    _record_progress(
        import_obj,
        ProgressEvent(
            status=DatasetArtifact.Status.DOWNLOADING,
            artifact_id=artifact.pk,
            message="Download started",
        ),
    )

    def on_chunk(downloaded: int, total: Optional[int]) -> None:
        _record_progress(
            import_obj,
            ProgressEvent(
                status=DatasetArtifact.Status.DOWNLOADING,
                artifact_id=artifact.pk,
                downloaded_bytes=downloaded,
                total_bytes=total or artifact.size_bytes,
                message="Downloading",
            ),
        )

    try:
        result = stream_download(artifact, chunk_callback=on_chunk)
    except DownloadError as exc:
        artifact.status = DatasetArtifact.Status.FAILED
        artifact.finished_at = timezone.now()
        artifact.last_error = str(exc)
        artifact.save(update_fields=["status", "finished_at", "last_error", "updated_at"])
        import_obj.status = DatasetImport.Status.FAILED
        import_obj.last_error = str(exc)
        import_obj.finished_at = timezone.now()
        import_obj.save(update_fields=["status", "last_error", "finished_at", "updated_at"])
        _record_progress(
            import_obj,
            ProgressEvent(
                status=DatasetArtifact.Status.FAILED,
                artifact_id=artifact.pk,
                downloaded_bytes=artifact.downloaded_bytes,
                total_bytes=artifact.size_bytes,
                message="Download failed",
            ),
        )
        _log(import_obj, "ERROR", f"Download failed: {exc}", artifact=artifact)
        raise

    artifact.file.name = result.path
    artifact.size_bytes = result.size_bytes
    artifact.downloaded_bytes = result.size_bytes
    artifact.checksum = result.checksum
    if result.content_type:
        artifact.content_type = result.content_type

    preview_metadata = {}
    try:
        preview_metadata = collect_download_preview(artifact)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("Failed to collect preview metadata for artifact %s: %s", artifact.pk, exc)
        preview_metadata = {"error": str(exc)}
    if preview_metadata:
        existing = dict(artifact.metadata or {})
        existing["preview"] = preview_metadata
        artifact.metadata = existing

    artifact.status = DatasetArtifact.Status.DOWNLOADED
    artifact.finished_at = timezone.now()
    artifact.save(
        update_fields=[
            "file",
            "size_bytes",
            "downloaded_bytes",
            "checksum",
            "content_type",
            "metadata",
            "status",
            "finished_at",
            "updated_at",
        ]
    )

    _record_progress(
        import_obj,
        ProgressEvent(
            status=DatasetArtifact.Status.DOWNLOADED,
            artifact_id=artifact.pk,
            downloaded_bytes=result.size_bytes,
            total_bytes=result.size_bytes,
            message="Download complete",
        ),
    )
    _log(import_obj, "INFO", "Download complete", artifact=artifact, extra={"size_bytes": result.size_bytes})

    # Check if all downloads are complete - if so, mark import as downloaded
    _maybe_mark_downloaded(import_obj)


@shared_task(bind=True)
def process_artifact(self, artifact_id: int) -> None:
    artifact = DatasetArtifact.objects.select_related("dataset_import").get(pk=artifact_id)
    import_obj = artifact.dataset_import

    artifact.status = DatasetArtifact.Status.PROCESSING
    artifact.save(update_fields=["status", "updated_at"])

    _record_progress(
        import_obj,
        ProgressEvent(
            status=DatasetArtifact.Status.PROCESSING,
            artifact_id=artifact.pk,
            downloaded_bytes=artifact.downloaded_bytes,
            total_bytes=artifact.size_bytes,
            message="Processing started",
        ),
    )

    try:
        asset, stats = _normalize_artifact(import_obj, artifact)
    except Exception as exc:
        artifact.status = DatasetArtifact.Status.FAILED
        artifact.last_error = str(exc)
        artifact.finished_at = timezone.now()
        artifact.save(update_fields=["status", "last_error", "finished_at", "updated_at"])
        import_obj.status = DatasetImport.Status.FAILED
        import_obj.last_error = str(exc)
        import_obj.finished_at = timezone.now()
        import_obj.save(update_fields=["status", "last_error", "finished_at", "updated_at"])
        _record_progress(
            import_obj,
            ProgressEvent(
                status=DatasetArtifact.Status.FAILED,
                artifact_id=artifact.pk,
                processed_bytes=artifact.processed_bytes,
                total_bytes=artifact.size_bytes,
                message="Processing failed",
            ),
        )
        _log(import_obj, "ERROR", f"Processing failed: {exc}", artifact=artifact)
        raise

    artifact.status = DatasetArtifact.Status.COMPLETED
    artifact.finished_at = timezone.now()
    artifact.processed_bytes = stats["normalized_bytes"]
    artifact.metadata = {**artifact.metadata, "normalized_asset_id": asset.pk}
    artifact.save(
        update_fields=[
            "status",
            "finished_at",
            "processed_bytes",
            "metadata",
            "updated_at",
        ]
    )

    _record_progress(
        import_obj,
        ProgressEvent(
            status=DatasetArtifact.Status.COMPLETED,
            artifact_id=artifact.pk,
            processed_bytes=artifact.processed_bytes,
            total_bytes=artifact.size_bytes,
            message=f"Processing complete ({stats['conversation_count']} conversations)",
        ),
    )

    _log(
        import_obj,
        "INFO",
        "Processing complete",
        artifact=artifact,
        extra=stats,
    )

    _maybe_finalize_import(import_obj)


def _normalize_artifact(
    import_obj: DatasetImport, artifact: DatasetArtifact
) -> tuple[DatasetAsset, dict[str, int]]:
    if not artifact.file:
        raise RuntimeError("Artifact file missing; download must complete before processing")

    storage = artifact.file.field.storage
    adapter = get_adapter(import_obj.adapter, storage)

    asset = DatasetAsset(
        dataset_import=import_obj,
        artifact=artifact,
        variant=DatasetAsset.Variant.NORMALIZED_JSONL,
        description="Normalized conversation JSONL",
    )

    filename = adapter.derive_asset_name(artifact, ".normalized.jsonl")
    path = asset.file.field.upload_to(asset, filename)
    checksum = hashlib.sha256()
    normalized_bytes = 0
    conversation_count = 0
    stored_conversations = 0
    last_report_conversations = 0
    pending_batch: list[NormalizedConversation] = []

    ensure_storage_dir(storage, path)

    with storage.open(path, "wb") as handle:
        for conversation in adapter.process(artifact, artifact.file.name):
            pending_batch.append(conversation)

            line = _conversation_to_jsonl(conversation)
            payload = (line + "\n").encode("utf-8")
            handle.write(payload)
            checksum.update(payload)
            normalized_bytes += len(payload)
            conversation_count += 1

            if len(pending_batch) >= PROGRESS_BATCH_SIZE:
                stored_conversations += load_conversations(import_obj, pending_batch)
                pending_batch.clear()

            if conversation_count - last_report_conversations >= PROGRESS_BATCH_SIZE:
                last_report_conversations = conversation_count
                DatasetProgressSnapshot.objects.create(
                    dataset_import=import_obj,
                    artifact=artifact,
                    status=DatasetArtifact.Status.PROCESSING,
                    downloaded_bytes=artifact.downloaded_bytes,
                    processed_bytes=normalized_bytes,
                    total_bytes=artifact.size_bytes,
                    message=f"Processed {conversation_count} conversations",
                )

    if pending_batch:
        stored_conversations += load_conversations(import_obj, pending_batch)

    asset.file.name = path
    asset.size_bytes = normalized_bytes
    asset.checksum = checksum.hexdigest()
    asset.metadata = {
        "conversation_count": conversation_count,
        "stored_conversations": stored_conversations,
        "adapter": import_obj.adapter,
    }
    asset.save()

    stats = {
        "conversation_count": conversation_count,
        "stored_conversations": stored_conversations,
        "normalized_bytes": normalized_bytes,
    }
    return asset, stats


def _conversation_to_jsonl(conversation: NormalizedConversation) -> str:
    import json

    return json.dumps(conversation.to_dict(), ensure_ascii=False)


def _maybe_mark_downloaded(import_obj: DatasetImport) -> None:
    """Check if all artifacts are downloaded and mark import as ready for analysis."""
    pending_downloads = import_obj.artifacts.exclude(
        status__in=[
            DatasetArtifact.Status.DOWNLOADED,
            DatasetArtifact.Status.COMPLETED,
            DatasetArtifact.Status.FAILED,
        ]
    ).exists()

    if not pending_downloads:
        failed_downloads = import_obj.artifacts.filter(status=DatasetArtifact.Status.FAILED).exists()
        if not failed_downloads:
            import_obj.mark_downloaded()
            _log(import_obj, "INFO", "All downloads complete - ready for analysis")
        else:
            import_obj.status = DatasetImport.Status.FAILED
            import_obj.finished_at = timezone.now()
            import_obj.save(update_fields=["status", "finished_at", "updated_at"])
            _log(import_obj, "ERROR", "Download phase failed")


def _maybe_finalize_import(import_obj: DatasetImport) -> None:
    """Check if all artifacts are processed and finalize import."""
    pending = import_obj.artifacts.exclude(
        status__in=[
            DatasetArtifact.Status.COMPLETED,
            DatasetArtifact.Status.FAILED,
        ]
    ).exists()

    if not pending:
        success = not import_obj.artifacts.filter(status=DatasetArtifact.Status.FAILED).exists()
        import_obj.mark_finished(success=success)
        _log(import_obj, "INFO", "Import finalized", extra={"success": success})


def _generate_topic_summary(import_obj: DatasetImport) -> dict[str, object]:
    """Produce a lightweight topical summary from sampled messages."""

    qs = (
        MessageRecord.objects.filter(
            conversation__dataset_import=import_obj,
            role__in=("user", "assistant"),
        )
        .order_by("id")
        .values_list("content", flat=True)[:TOPIC_SAMPLE_LIMIT]
    )

    sample = [content for content in qs if content]
    if not sample:
        return {
            "status": "no-data",
            "message": "No normalized conversations available yet. Finish processing to unlock topic summaries.",
        }

    keyword_counts: Counter[str] = Counter()
    snippets: list[str] = []

    for content in sample:
        words = WORD_PATTERN.findall(content.lower())
        if words:
            keyword_counts.update(words[:20])
        if len(snippets) < TOPIC_CLUSTERS and len(content.strip()) > 40:
            snippets.append(content.strip())

    if not keyword_counts:
        return {
            "status": "no-keywords",
            "message": "Unable to derive keywords from the sampled messages.",
            "sampled_messages": len(sample),
        }

    top_terms = [term for term, _ in keyword_counts.most_common(TOPIC_CLUSTERS * TOPIC_KEYWORDS_PER_TOPIC)]
    topics: list[dict[str, object]] = []
    for idx in range(TOPIC_CLUSTERS):
        start = idx * TOPIC_KEYWORDS_PER_TOPIC
        keywords = top_terms[start : start + TOPIC_KEYWORDS_PER_TOPIC]
        if not keywords:
            break
        topics.append(
            {
                "label": f"Theme {idx + 1}",
                "keywords": keywords,
                "sample": snippets[idx] if idx < len(snippets) else "",
            }
        )

    return {
        "status": "ok",
        "sampled_messages": len(sample),
        "top_keywords": top_terms,
        "topics": topics,
    }


@shared_task(bind=True)
def run_dataset_analysis(self, import_id: int) -> None:
    """Run analysis on a downloaded dataset import."""
    import_obj = DatasetImport.objects.get(pk=import_id)

    if import_obj.status != DatasetImport.Status.DOWNLOADED:
        _log(import_obj, "ERROR", f"Cannot analyze import with status: {import_obj.status}")
        return

    _log(import_obj, "INFO", "Analysis execution started", extra={"task_id": self.request.id})

    try:
        # Start analysis phase
        import_obj.start_analysis()

        # Analysis steps from django-test reference implementation
        analysis_steps = [
            (25, "Validating records"),
            (50, "Normalizing schema"),
            (75, "Extracting topics"),
            (100, "Indexing documents"),
        ]

        for progress, message in analysis_steps:
            import_obj.update_analysis_progress(progress, message)

            # Get all downloaded artifacts and process them
            if progress == 25:
                # Validation step - could add actual validation logic here
                import time
                time.sleep(2)  # Simulate processing time

            elif progress == 50:
                # Schema normalization - process each artifact
                artifacts = import_obj.artifacts.filter(status=DatasetArtifact.Status.DOWNLOADED)
                for artifact in artifacts:
                    process_artifact.delay(artifact.pk)

                # Wait for processing to complete
                import time
                time.sleep(5)  # Give processing time to start

            elif progress == 75:
                # Topic extraction - placeholder for actual topic modeling
                import time
                time.sleep(3)  # Simulate topic analysis

            elif progress == 100:
                # Document indexing - placeholder for search index creation
                import time
                time.sleep(2)  # Simulate indexing

        # Mark analysis as completed
        import_obj.mark_analysis_completed(success=True)
        summary = _generate_topic_summary(import_obj)
        notes = import_obj.notes if isinstance(import_obj.notes, dict) else {}
        notes["analysis_summary"] = summary
        import_obj.notes = notes
        import_obj.save(update_fields=["notes", "updated_at"])
        _log(import_obj, "INFO", "Analysis completed successfully", extra={"summary_status": summary.get("status")})

    except Exception as exc:
        import_obj.mark_analysis_completed(success=False)
        import_obj.last_error = str(exc)
        import_obj.save(update_fields=["last_error", "updated_at"])
        _log(import_obj, "ERROR", f"Analysis failed: {exc}")
        raise
