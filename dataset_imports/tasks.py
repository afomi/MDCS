from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
import io
import gzip
import bz2
import lzma
from pathlib import Path
import re
from typing import Optional

from celery import shared_task
from django.utils import timezone

from .adapters import get_adapter
from .constants import (
    DEFAULT_CLUSTER_COUNT,
    MAX_CLUSTER_DOCUMENTS,
    CLUSTER_PREVIEW_PER_GROUP,
)
from .models import (
    AnalysisJob,
    AnalysisResult,
    DatasetArtifact,
    DatasetAsset,
    DatasetImport,
    DatasetImportEvent,
    DatasetImportLog,
    DatasetProgressSnapshot,
    log_dataset_event,
    sync_import_aggregates,
)
from .normalization import NormalizedConversation, load_conversations
from .preview import collect_download_preview
from .services import DownloadError, ensure_storage_dir, stream_download

PROGRESS_BATCH_SIZE = 100

TOPIC_SAMPLE_LIMIT = 500
TOPIC_KEYWORDS_PER_TOPIC = 4
TOPIC_CLUSTERS = 3

WORD_PATTERN = re.compile(r"[A-Za-z0-9']+")

ANALYSIS_ALGORITHM_DEFAULT = "artifact-summary-v1"


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
        if not import_obj.events.filter(
            event_type=DatasetImportEvent.Type.DOWNLOAD_START
        ).exists():
            log_dataset_event(
                import_obj,
                DatasetImportEvent.Type.DOWNLOAD_START,
                details={"artifact_id": event.artifact_id},
            )


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
    word_count = 0
    pending_batch: list[NormalizedConversation] = []

    ensure_storage_dir(storage, path)

    with storage.open(path, "wb") as handle:
        # Enforce optional per-artifact conversation cap from import notes
        max_conversations = None
        try:
            if isinstance(import_obj.notes, dict):
                raw_limits = import_obj.notes.get("normalize_limits") or {}
                if isinstance(raw_limits, dict):
                    raw = raw_limits.get(str(artifact.pk)) or raw_limits.get(artifact.pk)
                    if raw is not None:
                        max_conversations = int(raw)
        except Exception:
            max_conversations = None

        for conversation in adapter.process(artifact, artifact.file.name):
            pending_batch.append(conversation)

            line = _conversation_to_jsonl(conversation)
            payload = (line + "\n").encode("utf-8")
            handle.write(payload)
            checksum.update(payload)
            normalized_bytes += len(payload)
            conversation_count += 1

            if max_conversations and conversation_count >= max_conversations:
                # Flush any pending and stop early per limit
                if pending_batch:
                    stored_conversations += load_conversations(import_obj, pending_batch)
                    pending_batch.clear()
                break

            for message in conversation.messages:
                if message.content:
                    tokens = _tokenize(message.content)
                    word_count += len(tokens)

            if len(pending_batch) >= PROGRESS_BATCH_SIZE:
                stored_conversations += load_conversations(import_obj, pending_batch)
                pending_batch.clear()

            if conversation_count - last_report_conversations >= PROGRESS_BATCH_SIZE:
                last_report_conversations = conversation_count
                # Persist a lightweight progress snapshot and update artifact aggregates
                DatasetProgressSnapshot.objects.create(
                    dataset_import=import_obj,
                    artifact=artifact,
                    status=DatasetArtifact.Status.PROCESSING,
                    downloaded_bytes=artifact.downloaded_bytes,
                    processed_bytes=normalized_bytes,
                    total_bytes=artifact.size_bytes,
                    message=f"Processed {conversation_count} conversations",
                )
                _record_progress(
                    import_obj,
                    ProgressEvent(
                        status=DatasetArtifact.Status.PROCESSING,
                        artifact_id=artifact.pk,
                        downloaded_bytes=artifact.downloaded_bytes,
                        processed_bytes=normalized_bytes,
                        total_bytes=artifact.size_bytes,
                        message="Processing",
                    ),
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
        "word_count": word_count,
    }
    asset.save()

    stats = {
        "conversation_count": conversation_count,
        "stored_conversations": stored_conversations,
        "normalized_bytes": normalized_bytes,
        "word_count": word_count,
    }
    return asset, stats


def _conversation_to_jsonl(conversation: NormalizedConversation) -> str:
    import json

    return json.dumps(conversation.to_dict(), ensure_ascii=False)


def _tokenize(text: str) -> list[str]:
    if not text:
        return []
    return [token for token in WORD_PATTERN.findall(text.lower()) if token]


def _analysis_cancelled(import_obj: DatasetImport) -> bool:
    import_obj.refresh_from_db(fields=["analysis_status", "status", "analysis_message"])
    return import_obj.analysis_status == "cancelled" or import_obj.status == DatasetImport.Status.DOWNLOADED


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
            if not import_obj.events.filter(
                event_type=DatasetImportEvent.Type.DOWNLOAD_COMPLETE
            ).exists():
                log_dataset_event(
                    import_obj,
                    DatasetImportEvent.Type.DOWNLOAD_COMPLETE,
                )
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


def _inspect_artifact(artifact: DatasetArtifact) -> dict[str, object]:
    stats: dict[str, object] = {
        "filename": artifact.filename,
        "status": artifact.status,
        "downloaded_bytes": artifact.downloaded_bytes,
        "size_bytes": artifact.size_bytes,
    }

    if isinstance(artifact.metadata, dict):
        preview = artifact.metadata.get("preview") or {}
        if isinstance(preview, dict) and "row_count" in preview:
            stats["row_count"] = preview.get("row_count")

    file_field = artifact.file
    if file_field and file_field.name:
        try:
            storage = file_field.storage
            with storage.open(file_field.name, "rb") as handle:
                chunk = handle.read(1024 * 64)
                stats["sample_bytes"] = len(chunk)
                if chunk:
                    stats["sample_checksum"] = hashlib.sha256(chunk).hexdigest()
        except Exception as exc:  # pragma: no cover - best effort diagnostics
            stats["read_error"] = str(exc)

    normalized_asset = (
        artifact.assets.filter(variant=DatasetAsset.Variant.NORMALIZED_JSONL)
        .order_by("-created_at")
        .first()
    )
    if normalized_asset and isinstance(normalized_asset.metadata, dict):
        meta = normalized_asset.metadata
        if meta.get("conversation_count") is not None and "row_count" not in stats:
            stats["row_count"] = meta.get("conversation_count")
        if meta.get("word_count") is not None:
            stats["word_count"] = meta.get("word_count")

    fallback_stats = _compute_artifact_stats_fallback(artifact)
    for key, value in fallback_stats.items():
        if key not in stats and value is not None:
            stats[key] = value

    return stats


def _compute_artifact_stats_fallback(artifact: DatasetArtifact) -> dict[str, int | None]:
    """Best-effort metrics when normalized metadata is unavailable."""

    file_field = artifact.file
    if not file_field or not file_field.name:
        return {}

    suffix = Path(artifact.filename or "").suffix.lower()

    def _text_stream(raw_handle):
        if suffix in {".gz", ".gzip"}:
            return io.TextIOWrapper(gzip.GzipFile(fileobj=raw_handle), encoding="utf-8", errors="ignore")
        if suffix in {".bz2", ".bzip2"}:
            return io.TextIOWrapper(bz2.BZ2File(raw_handle), encoding="utf-8", errors="ignore")
        if suffix in {".xz", ".lzma"}:
            return io.TextIOWrapper(lzma.LZMAFile(raw_handle), encoding="utf-8", errors="ignore")
        return io.TextIOWrapper(raw_handle, encoding="utf-8", errors="ignore")

    try:
        storage = file_field.storage
        with storage.open(file_field.name, "rb") as raw_handle:
            with _text_stream(raw_handle) as reader:
                word_total = 0
                row_total = 0
                for line in reader:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    row_total += 1
                    word_total += len(_tokenize(stripped))
        return {
            "row_count": row_total if row_total > 0 else None,
            "word_count": word_total if word_total > 0 else None,
        }
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("Failed to compute artifact stats fallback for artifact %s", artifact.pk, exc_info=True)
        return {}


def _run_topic_clustering(
    import_obj: DatasetImport,
    *,
    cluster_count: int,
    max_documents: int = MAX_CLUSTER_DOCUMENTS,
) -> dict[str, object]:
    """Cluster conversation documents for conversational datasets (e.g., OASST1)."""

    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.cluster import MiniBatchKMeans
    except ImportError as exc:  # pragma: no cover - library guard
        logger.error("Clustering dependencies missing: %s", exc)
        return {}

    documents: list[str] = []
    conversation_refs: list[dict[str, object]] = []

    conversations_qs = (
        import_obj.conversations.order_by("id").prefetch_related("messages")
    )

    for conversation in conversations_qs[:max_documents]:
        texts = [
            message.content.strip()
            for message in conversation.messages.all()
            if message.content
        ]
        if not texts:
            continue

        combined_text = " ".join(texts)
        if not combined_text.strip():
            continue

        documents.append(combined_text)
        conversation_refs.append(
            {
                "source_id": conversation.source_id,
                "title": conversation.title or f"Conversation {conversation.pk}",
                "preview": texts[0][:240],
            }
        )

    document_count = len(documents)
    if document_count < 2:
        return {
            "document_count": document_count,
            "cluster_count": 0,
            "clusters": [],
            "vectorizer": "tfidf",
            "algorithm": "minibatch-kmeans",
            "status": "insufficient-documents",
            "note": "At least two conversations are required to form topic clusters.",
        }

    effective_clusters = max(2, min(cluster_count, document_count))

    vectorizer = TfidfVectorizer(max_features=5000, stop_words="english")
    matrix = vectorizer.fit_transform(documents)

    kmeans = MiniBatchKMeans(
        n_clusters=effective_clusters,
        random_state=42,
        batch_size=256,
        n_init="auto",
    )
    labels = kmeans.fit_predict(matrix)
    feature_names = vectorizer.get_feature_names_out()

    clusters: list[dict[str, object]] = []
    for cluster_idx in range(effective_clusters):
        member_indices = [
            idx for idx, label in enumerate(labels) if label == cluster_idx
        ]
        if not member_indices:
            continue

        centroid = kmeans.cluster_centers_[cluster_idx]
        top_term_indices = centroid.argsort()[-10:][::-1]
        top_terms = [
            feature_names[idx]
            for idx in top_term_indices
            if centroid[idx] > 0
        ]

        samples: list[dict[str, object]] = []
        for doc_idx in member_indices[:CLUSTER_PREVIEW_PER_GROUP]:
            ref = conversation_refs[doc_idx]
            samples.append(
                {
                    "title": ref["title"],
                    "source_id": ref["source_id"],
                    "excerpt": ref["preview"],
                }
            )

        clusters.append(
            {
                "cluster_id": cluster_idx,
                "size": len(member_indices),
                "top_terms": top_terms[:10],
                "sample_conversations": samples,
            }
        )

    clusters.sort(key=lambda item: item["size"], reverse=True)

    if not clusters:
        return {
            "document_count": document_count,
            "cluster_count": 0,
            "clusters": [],
            "vectorizer": "tfidf",
            "algorithm": "minibatch-kmeans",
            "status": "no-clusters",
            "note": "Clustering finished but produced no groups. Try a smaller cluster count or ensure conversations contain diverse content.",
        }

    return {
        "document_count": document_count,
        "cluster_count": effective_clusters,
        "clusters": clusters,
        "vectorizer": "tfidf",
        "algorithm": "minibatch-kmeans",
        "status": "ok",
    }


@shared_task(bind=True)
def run_dataset_analysis(self, import_id: int, job_id: int | None = None) -> None:
    """Run a lightweight analysis over downloaded artifacts."""

    import_obj = DatasetImport.objects.get(pk=import_id)

    allowed_statuses = {
        DatasetImport.Status.DOWNLOADED,
        DatasetImport.Status.COMPLETED,
    }

    if import_obj.status not in allowed_statuses:
        _log(import_obj, "ERROR", f"Cannot analyze import with status: {import_obj.status}")
        return

    task_id = self.request.id
    _log(import_obj, "INFO", "Analysis execution started", extra={"task_id": task_id})

    notes = import_obj.notes if isinstance(import_obj.notes, dict) else {}
    notes = dict(notes or {})

    requested_mode = notes.get("analysis_mode") or ANALYSIS_ALGORITHM_DEFAULT
    cluster_pref = notes.get("analysis_cluster_count")
    try:
        cluster_pref_int = int(cluster_pref) if cluster_pref is not None else None
    except (TypeError, ValueError):
        cluster_pref_int = None

    job: AnalysisJob | None = None
    if job_id:
        job = AnalysisJob.objects.filter(pk=job_id, dataset_import=import_obj).first()
    if not job:
        job = AnalysisJob.objects.create(
            dataset_import=import_obj,
            requested_mode=requested_mode,
            requested_cluster_count=cluster_pref_int,
            status=AnalysisJob.Status.RUNNING,
        )
        job_id = job.pk

    job.task_id = task_id
    job.save(update_fields=["task_id", "updated_at"])

    notes["analysis_job_id"] = job_id
    import_obj.notes = notes
    import_obj.save(update_fields=["notes", "updated_at"])

    cancelled = False
    try:
        import_obj.start_analysis()
        import_obj.set_analysis_task_id(task_id)
        log_dataset_event(
            import_obj,
            DatasetImportEvent.Type.ANALYSIS_START,
            details={"task_id": task_id},
        )

        artifacts = list(
            import_obj.artifacts.filter(
                status__in=[
                    DatasetArtifact.Status.DOWNLOADED,
                    DatasetArtifact.Status.COMPLETED,
                ]
            ).order_by("sequence", "id")
        )

        total_artifacts = len(artifacts)
        summary_rows: list[dict[str, object]] = []
        total_bytes = 0

        if not artifacts:
            import_obj.update_analysis_progress(25, "No downloaded artifacts to inspect")

        for index, artifact in enumerate(artifacts, start=1):
            if _analysis_cancelled(import_obj):
                cancelled = True
                break

            progress = int(((index - 1) / max(total_artifacts, 1)) * 100)
            message = f"Inspecting {artifact.filename}"
            import_obj.update_analysis_progress(progress, message)
            _log(import_obj, "INFO", message, extra={"artifact_id": artifact.pk})

            stats = _inspect_artifact(artifact)
            summary_rows.append(stats)
            total_bytes += artifact.downloaded_bytes

            if _analysis_cancelled(import_obj):
                cancelled = True
                break

            progress_after = int((index / max(total_artifacts, 1)) * 100)
            import_obj.update_analysis_progress(progress_after, f"Finished {artifact.filename}")

        if cancelled or _analysis_cancelled(import_obj):
            _log(import_obj, "INFO", "Analysis cancelled before completion")
            return

        import_obj.update_analysis_progress(100, "Aggregating results")

        row_total = 0
        word_total = 0

        for row in summary_rows:
            row_total += int(row.get("row_count") or 0)
            word_total += int(row.get("word_count") or 0)

        summary: dict[str, object] = {
            "status": "ok",
            "artifact_count": total_artifacts,
            "total_downloaded_bytes": total_bytes,
            "artifacts": summary_rows,
            "total_row_count": row_total,
            "total_word_count": word_total,
        }

        notes = import_obj.notes if isinstance(import_obj.notes, dict) else {}

        algorithm = notes.get("analysis_mode") or ANALYSIS_ALGORITHM_DEFAULT
        dataset_identifier = notes.get("dataset_id") or import_obj.adapter

        if algorithm == "topic-clusters":
            # Topic clustering is supported for any dataset that yields normalized
            # conversations; skip the expensive work if no conversations exist yet.
            if import_obj.conversations.exists():
                requested_clusters = notes.get("analysis_cluster_count")
                try:
                    requested_clusters = int(requested_clusters)
                except (TypeError, ValueError):
                    requested_clusters = DEFAULT_CLUSTER_COUNT

                requested_clusters = max(2, min(requested_clusters, 50))
                summary["requested_cluster_count"] = requested_clusters

                # Document limit for clustering (cap between 100 and 20000 for memory safety)
                requested_doc_limit = notes.get("analysis_cluster_doc_limit")
                try:
                    requested_doc_limit = int(requested_doc_limit)
                except (TypeError, ValueError):
                    requested_doc_limit = MAX_CLUSTER_DOCUMENTS
                requested_doc_limit = max(100, min(requested_doc_limit, 20000))
                summary["requested_document_limit"] = requested_doc_limit

                clustering = _run_topic_clustering(
                    import_obj,
                    cluster_count=requested_clusters,
                    max_documents=requested_doc_limit,
                )

                clusters = clustering.get("clusters") or []
                summary["cluster_overview"] = {
                    "cluster_count": clustering.get("cluster_count", len(clusters)),
                    "document_count": clustering.get("document_count", 0),
                    "vectorizer": clustering.get("vectorizer"),
                    "algorithm": clustering.get("algorithm"),
                    "status": clustering.get("status"),
                    "note": clustering.get("note"),
                    "requested_count": requested_clusters,
                }
                summary["clusters"] = clusters
                if job:
                    job.cluster_count = clustering.get("cluster_count")
                    job.document_count = clustering.get("document_count")
            else:
                requested_clusters = notes.get("analysis_cluster_count") or DEFAULT_CLUSTER_COUNT
                summary["requested_cluster_count"] = requested_clusters
                summary["cluster_overview"] = {
                    "cluster_count": 0,
                    "document_count": 0,
                    "status": "no-conversations",
                    "note": "Topic clustering requires normalized conversations. Rerun analysis after processing completes.",
                    "requested_count": requested_clusters,
                }
                summary["clusters"] = []

        if job:
            job.requested_mode = requested_mode
            # Preserve existing requested_cluster_count for non-clustering modes.
            requested = summary.get("requested_cluster_count")
            if algorithm == "topic-clusters" and requested is not None:
                job.requested_cluster_count = requested
            if algorithm == "topic-clusters":
                job.mark_complete(
                    algorithm=summary.get("cluster_overview", {}).get("algorithm") or algorithm,
                    clusters=summary.get("cluster_overview", {}).get("cluster_count"),
                    documents=summary.get("cluster_overview", {}).get("document_count"),
                    metadata={
                        "summary_status": summary.get("status"),
                        "cluster_overview": summary.get("cluster_overview"),
                    },
                )
            else:
                # For non-clustering algorithms, report the total document count and requested clusters (if any)
                doc_count = import_obj.conversations.count()
                job.mark_complete(
                    algorithm=algorithm,
                    clusters=job.requested_cluster_count,
                    documents=doc_count,
                    metadata={
                        "summary_status": summary.get("status"),
                    },
                )

        notes["analysis_summary"] = summary
        notes.pop("analysis_job_id", None)
        import_obj.notes = notes
        import_obj.save(update_fields=["notes", "updated_at"])

        AnalysisResult.objects.update_or_create(
            dataset_import=import_obj,
            defaults={
                "dataset_id": dataset_identifier,
                "algorithm": algorithm,
                "summary": summary,
            },
        )

        import_obj.mark_analysis_completed(success=True)
        log_dataset_event(
            import_obj,
            DatasetImportEvent.Type.ANALYSIS_COMPLETE,
            details={
                "artifact_count": total_artifacts,
                "total_downloaded_bytes": total_bytes,
            },
        )
        _log(import_obj, "INFO", "Analysis completed successfully", extra={"artifact_count": total_artifacts})

    except Exception as exc:
        import_obj.mark_analysis_completed(success=False)
        import_obj.last_error = str(exc)
        import_obj.save(update_fields=["last_error", "updated_at"])
        if job:
            job.mark_failed(str(exc))
        notes.pop("analysis_job_id", None)
        import_obj.notes = notes
        import_obj.save(update_fields=["notes", "updated_at"])
        log_dataset_event(
            import_obj,
            DatasetImportEvent.Type.ANALYSIS_ERROR,
            message=str(exc),
        )
        _log(import_obj, "ERROR", f"Analysis failed: {exc}")
        raise
    finally:
        import_obj.set_analysis_task_id(None)
        if job and job.status == AnalysisJob.Status.RUNNING:
            job.mark_cancelled("Analysis terminated before completion")
        if notes.pop("analysis_job_id", None) is not None:
            import_obj.notes = notes
            import_obj.save(update_fields=["notes", "updated_at"])
