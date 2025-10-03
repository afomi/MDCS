from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from django.contrib import messages
from django.db import transaction
from django.http import (
    Http404,
    HttpRequest,
    HttpResponse,
    JsonResponse,
    StreamingHttpResponse,
)
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.text import slugify
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.middleware.csrf import get_token

from mdcs.celery import app as celery_app

from .catalog import ArtifactDefinition, DatasetDefinition, get_dataset, list_datasets
from .constants import DEFAULT_CLUSTER_COUNT
from .models import (
    AnalysisJob,
    DatasetArtifact,
    DatasetImport,
    DatasetImportEvent,
    DatasetImportLog,
    MessageRecord,
    log_dataset_event,
    reconcile_import_status,
    sync_import_aggregates,
)
from .tasks import process_artifact, run_dataset_import, run_dataset_analysis

DEFAULT_STATUS = {
    "label": "Remote (not downloaded)",
    "badge_class": "status-tag--neutral",
    "show_check": False,
}

ANALYSIS_MODES: Tuple[Tuple[str, str], ...] = (
    ("top-down", "Top-down (seeded topics)"),
    ("bottom-up", "Bottom-up (clustering-first)"),
    ("hybrid", "Hybrid (combine both)"),
    ("topic-clusters", "Topic clusters (k-means)"),
)
DEFAULT_ANALYSIS_MODE = ANALYSIS_MODES[0][0]

STATUS_METADATA = {
    DatasetImport.Status.PENDING: {
        "label": "Queued",
        "badge_class": "status-tag--neutral",
        "show_check": False,
    },
    DatasetImport.Status.QUEUED: {
        "label": "Queued",
        "badge_class": "status-tag--info",
        "show_check": False,
    },
    DatasetImport.Status.DOWNLOADING: {
        "label": "Downloading",
        "badge_class": "status-tag--info",
        "show_check": False,
    },
    DatasetImport.Status.PROCESSING: {
        "label": "Processing",
        "badge_class": "status-tag--warning",
        "show_check": False,
    },
    DatasetImport.Status.COMPLETED: {
        "label": "Completed",
        "badge_class": "status-tag--success",
        "show_check": True,
    },
    DatasetImport.Status.DOWNLOADED: {
        "label": "Downloaded",
        "badge_class": "status-tag--success",
        "show_check": True,
    },
    DatasetImport.Status.ANALYZING: {
        "label": "Analyzing",
        "badge_class": "status-tag--info",
        "show_check": False,
    },
    DatasetImport.Status.FAILED: {
        "label": "Failed",
        "badge_class": "status-tag--error",
        "show_check": False,
    },
    DatasetImport.Status.CANCELLED: {
        "label": "Cancelled",
        "badge_class": "status-tag--neutral",
        "show_check": False,
    },
    DatasetArtifact.Status.PENDING: {
        "label": "Pending",
        "badge_class": "status-tag--neutral",
        "show_check": False,
    },
    DatasetArtifact.Status.DOWNLOADING: {
        "label": "Downloading",
        "badge_class": "status-tag--info",
        "show_check": False,
    },
    DatasetArtifact.Status.DOWNLOADED: {
        "label": "Downloaded",
        "badge_class": "status-tag--success",
        "show_check": True,
    },
    DatasetArtifact.Status.PROCESSING: {
        "label": "Processing",
        "badge_class": "status-tag--warning",
        "show_check": False,
    },
    DatasetArtifact.Status.COMPLETED: {
        "label": "Completed",
        "badge_class": "status-tag--success",
        "show_check": True,
    },
    DatasetArtifact.Status.FAILED: {
        "label": "Failed",
        "badge_class": "status-tag--error",
        "show_check": False,
    },
    "not-requested": {
        "label": "Not requested",
        "badge_class": "status-tag--muted",
        "show_check": False,
    },
}

def _get_status_metadata(status: str | None) -> Dict[str, object]:
    if status and status in STATUS_METADATA:
        return STATUS_METADATA[status]
    return DEFAULT_STATUS


def catalog_view(request: HttpRequest) -> HttpResponse:
    get_token(request)
    datasets = []
    for dataset in list_datasets():
        latest_import = (
            DatasetImport.objects.filter(notes__dataset_id=dataset.id)
            .order_by("-created_at")
            .first()
        )
        status = latest_import.status if latest_import else None
        status_meta = _get_status_metadata(status)
        total_artifacts_defined = len(dataset.artifacts)
        artifact_limit = (
            latest_import.notes.get("artifact_limit")
            if latest_import and isinstance(latest_import.notes, dict)
            else None
        )
        if not artifact_limit:
            artifact_limit = total_artifacts_defined

        datasets.append(
            {
                "dataset": dataset,
                "latest_import": latest_import,
                "status": status_meta["label"],
                "status_slug": status or "not-started",
                "status_badge_class": status_meta["badge_class"],
                "status_show_check": status_meta["show_check"],
                "detail_url": reverse("dataset_imports:detail", args=[dataset.id]),
                "status_url": reverse("dataset_imports:status", args=[dataset.id]),
                "artifact_limit": artifact_limit,
                "total_artifacts_defined": total_artifacts_defined,
            }
        )

    context = {
        "datasets": datasets,
        "user_is_staff": request.user.is_staff,
    }
    return render(request, "dataset_imports/catalog.html", context)


def explore_view(request: HttpRequest) -> HttpResponse:
    get_token(request)
    dataset_entries = []
    selected_dataset_id = (request.GET.get("dataset") or "").strip()
    active_entry = None
    for dataset in list_datasets():
        latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )
        state = _serialize_import_state(dataset, latest_import)
        entry = {
            "dataset": dataset,
            "state": state,
            "status_url": reverse("dataset_imports:status", args=[dataset.id]),
            "analyze_url": reverse("dataset_imports:analyze", args=[dataset.id]),
            "detail_url": reverse("dataset_imports:detail", args=[dataset.id]),
            "selected": dataset.id == selected_dataset_id,
        }
        dataset_entries.append(entry)
        if entry["selected"]:
            active_entry = entry

    context = {
        "datasets": dataset_entries,
        "analysis_modes": ANALYSIS_MODES,
        "user_is_staff": request.user.is_staff,
        "selected_dataset_id": selected_dataset_id,
        "active_entry": active_entry,
        "default_cluster_count": DEFAULT_CLUSTER_COUNT,
    }
    return render(request, "dataset_imports/explore.html", context)


def dataset_detail(request: HttpRequest, dataset_id: str) -> HttpResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError as exc:  # pragma: no cover - defensive
        raise Http404(str(exc))

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts", "events")
        .order_by("-created_at")
        .first()
    )

    pipeline_steps: list[dict[str, object]] = []
    if latest_import:
        artifact_statuses = list(
            latest_import.artifacts.values_list("status", flat=True)
        )
        if artifact_statuses:
            completed_download_states = {
                DatasetArtifact.Status.DOWNLOADED,
                DatasetArtifact.Status.COMPLETED,
                DatasetArtifact.Status.FAILED,
            }
            download_complete = all(
                status in completed_download_states for status in artifact_statuses
            )
            has_failures = DatasetArtifact.Status.FAILED in artifact_statuses
            if download_complete and latest_import.status in {
                DatasetImport.Status.PENDING,
                DatasetImport.Status.QUEUED,
                DatasetImport.Status.DOWNLOADING,
            }:
                reconcile_import_status(
                    latest_import,
                    download_complete=True,
                    has_failures=has_failures,
                )
                latest_import.refresh_from_db()
    dataset_completed = bool(
        latest_import and latest_import.status == DatasetImport.Status.COMPLETED
    )

    can_launch = dataset.supported and not dataset_completed and (
        latest_import is None
        or latest_import.status
        in {
            DatasetImport.Status.FAILED,
            DatasetImport.Status.CANCELLED,
        }
    )

    total_artifacts_defined = len(dataset.artifacts)
    notes: dict[str, object] = {}
    if latest_import and isinstance(latest_import.notes, dict):
        notes = latest_import.notes

    selected_artifacts = [
        str(name)
        for name in (notes.get("selected_artifacts") if notes else [])
        if str(name).strip()
    ]
    if not selected_artifacts:
        selected_artifacts = [artifact.filename for artifact in dataset.artifacts]

    artifact_rows = _build_artifact_rows(
        dataset,
        latest_import,
        selected_artifacts=selected_artifacts,
    )

    latest_analysis_mode = notes.get("analysis_mode") if notes else None
    latest_artifact_limit = notes.get("artifact_limit") if notes else None

    downloaded_artifact_count = 0
    normalized_artifact_count = 0
    if latest_import:
        downloaded_artifact_count = latest_import.artifacts.filter(
            status__in=[
                DatasetArtifact.Status.DOWNLOADED,
                DatasetArtifact.Status.COMPLETED,
            ]
        ).count()
        normalized_artifact_count = latest_import.artifacts.filter(
            status=DatasetArtifact.Status.COMPLETED
        ).count()

    analysis_selection_enabled = bool(latest_import) and normalized_artifact_count > 0

    analysis_cluster_count = DEFAULT_CLUSTER_COUNT
    if isinstance(notes, dict):
        cluster_raw_value = notes.get("analysis_cluster_count")
        try:
            analysis_cluster_count = int(cluster_raw_value)
        except (TypeError, ValueError):
            analysis_cluster_count = DEFAULT_CLUSTER_COUNT
    analysis_cluster_count = max(2, min(analysis_cluster_count, 50))

    # Topic clustering document limit (UI + analysis)
    analysis_doc_limit = None
    if isinstance(notes, dict):
        doc_limit_raw = notes.get("analysis_cluster_doc_limit")
        try:
            analysis_doc_limit = int(doc_limit_raw) if doc_limit_raw is not None else None
        except (TypeError, ValueError):
            analysis_doc_limit = None
    if analysis_doc_limit is None:
        analysis_doc_limit = 1500
    analysis_doc_limit = max(100, min(analysis_doc_limit, 20000))


    can_analyze = (
        dataset.supported
        and latest_import is not None
        and latest_import.status
        in {
            DatasetImport.Status.DOWNLOADED,
            DatasetImport.Status.COMPLETED,
        }
        and normalized_artifact_count > 0
        and latest_import.analysis_status != "running"
    )
    is_analyzing = (
        latest_import is not None
        and latest_import.status == DatasetImport.Status.ANALYZING
    )

    if request.method == "POST":
        action = request.POST.get("action") or "start-import"

        if action == "update-analysis":
            if not request.user.is_staff:
                messages.error(request, "Only staff users can update the analysis approach.")
                return redirect("dataset_imports:detail", dataset_id=dataset.id)

            if not latest_import:
                messages.warning(request, "No import available yet. Start an import before selecting an analysis approach.")
                return redirect("dataset_imports:detail", dataset_id=dataset.id)

            if not analysis_selection_enabled:
                messages.warning(request, "Download at least one file before applying an analysis approach.")
                return redirect("dataset_imports:detail", dataset_id=dataset.id)

            requested_analysis_mode = (request.POST.get("analysis_mode") or "").strip().lower()
            valid_modes = {mode for mode, _ in ANALYSIS_MODES}
            if requested_analysis_mode not in valid_modes:
                requested_analysis_mode = DEFAULT_ANALYSIS_MODE

            cluster_raw = (request.POST.get("cluster_count") or "").strip()
            try:
                cluster_value = int(cluster_raw) if cluster_raw else DEFAULT_CLUSTER_COUNT
            except ValueError:
                cluster_value = DEFAULT_CLUSTER_COUNT
            cluster_value = max(2, min(cluster_value, 50))

            doc_limit_raw = (request.POST.get("document_limit") or "").strip()
            try:
                doc_limit_value = int(doc_limit_raw) if doc_limit_raw else 1500
            except ValueError:
                doc_limit_value = 1500
            doc_limit_value = max(100, min(doc_limit_value, 20000))

            notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
            notes["analysis_mode"] = requested_analysis_mode
            notes["analysis_cluster_count"] = cluster_value
            notes["analysis_cluster_doc_limit"] = doc_limit_value
            latest_import.notes = notes
            latest_import.save(update_fields=["notes", "updated_at"])
            messages.success(request, "Analysis approach updated. Redirecting to explore view…")
            explore_url = f"{reverse('dataset_imports:explore')}?dataset={dataset.id}"
            return redirect(explore_url)

        if action == "reset-import":
            if not request.user.is_staff:
                messages.error(request, "Only staff users can reset imports.")
                return redirect("dataset_imports:detail", dataset_id=dataset.id)

            if not latest_import:
                messages.info(request, "No import to reset yet.")
                return redirect("dataset_imports:detail", dataset_id=dataset.id)

            with transaction.atomic():
                for asset in latest_import.assets.all():
                    if asset.file:
                        asset.file.delete(save=False)
                for artifact in latest_import.artifacts.all():
                    if artifact.file:
                        artifact.file.delete(save=False)
                latest_import.delete()

            messages.success(request, "Import reset. Start a new run when ready.")
            return redirect("dataset_imports:detail", dataset_id=dataset.id)

        if not dataset.supported:
            messages.error(request, "Ingestion pipeline not yet configured for this dataset.")
            return redirect("dataset_imports:detail", dataset_id=dataset.id)
        if not request.user.is_staff:
            messages.error(request, "Only staff users can launch dataset imports.")
            return redirect("dataset_imports:detail", dataset_id=dataset.id)
        if not can_launch:
            if dataset_completed:
                messages.warning(
                    request,
                    "Import already completed. Reset the dataset before running a new import.",
                )
            else:
                messages.warning(
                    request,
                    "An import is already running for this dataset.",
                )
            return redirect("dataset_imports:detail", dataset_id=dataset.id)

        raw_selected = [
            str(value).strip() for value in request.POST.getlist("selected_artifacts") if str(value).strip()
        ]
        selected_override: list[str] | None = None
        if raw_selected:
            defined_lookup = {artifact.filename for artifact in dataset.artifacts}
            selected_lookup = {value for value in raw_selected if value in defined_lookup}
            selected_order = [
                artifact.filename
                for artifact in dataset.artifacts
                if artifact.filename in selected_lookup
            ]
            if not selected_order:
                messages.error(request, "Select at least one artifact before starting an import.")
                return redirect("dataset_imports:detail", dataset_id=dataset.id)
            selected_override = selected_order

        requested_analysis_mode = (
            (request.POST.get("analysis_mode") or "").strip().lower()
            or latest_analysis_mode
            or DEFAULT_ANALYSIS_MODE
        )
        valid_modes = {mode for mode, _ in ANALYSIS_MODES}
        if requested_analysis_mode not in valid_modes:
            requested_analysis_mode = DEFAULT_ANALYSIS_MODE

        limit_raw = request.POST.get("artifact_limit")
        artifact_limit = total_artifacts_defined
        if limit_raw:
            try:
                artifact_limit = int(limit_raw)
            except (TypeError, ValueError):
                artifact_limit = total_artifacts_defined
        if total_artifacts_defined:
            artifact_limit = max(1, min(artifact_limit, total_artifacts_defined))
        else:
            artifact_limit = 0

        if selected_override is not None:
            artifact_limit = len(selected_override)

        cluster_raw = (request.POST.get("cluster_count") or str(analysis_cluster_count))
        try:
            start_cluster_count = int(cluster_raw)
        except (TypeError, ValueError):
            start_cluster_count = DEFAULT_CLUSTER_COUNT
        start_cluster_count = max(2, min(start_cluster_count, 50))

        import_obj = _create_import_for_dataset(
            request.user.id,
            dataset,
            analysis_mode=requested_analysis_mode,
            artifact_limit=artifact_limit,
            cluster_count=start_cluster_count,
            selected_artifacts=selected_override,
        )
        sync_import_aggregates(import_obj)
        try:
            run_dataset_import.delay(import_obj.pk)
            messages.success(request, "Import queued. Background workers will begin downloading shortly.")
        except Exception as exc:  # pragma: no cover - celery misconfiguration
            messages.error(request, f"Import created but Celery dispatch failed: {exc}")
        return redirect("dataset_imports:detail", dataset_id=dataset.id)

    import_status = latest_import.status if latest_import else None
    import_status_meta = _get_status_metadata(import_status)

    if (
        latest_import
        and import_status in {DatasetImport.Status.PENDING, DatasetImport.Status.QUEUED}
        and downloaded_artifact_count == 0
        and initial_downloaded_bytes == 0
    ):
        import_status_meta = DEFAULT_STATUS

    analysis_mode_labels = dict(ANALYSIS_MODES)

    latest_analysis_label = (
        analysis_mode_labels.get(latest_analysis_mode) if latest_analysis_mode else None
    )

    total_artifacts_display = total_artifacts_defined
    download_progress_pct = 0
    initial_downloaded_bytes = sum(row["downloaded_bytes"] for row in artifact_rows)
    downloaded_bytes_total = initial_downloaded_bytes
    processed_bytes_total = sum(row["processed_bytes"] for row in artifact_rows)
    total_bytes_expected = 0

    if latest_import:
        total_artifacts_display = latest_import.total_artifacts or total_artifacts_defined

    analysis_summary = None
    analysis_algorithm = None
    analysis_algorithm_label = None
    is_normalizing = False
    can_normalize = False
    if latest_import:
        analysis_summary = notes.get("analysis_summary") if notes else None
        analysis_algorithm = notes.get("analysis_mode") if notes else None
        result = getattr(latest_import, "analysis_result", None)
        if result:
            if not analysis_summary:
                analysis_summary = result.summary
            if result.algorithm:
                analysis_algorithm = result.algorithm

        if analysis_algorithm:
            analysis_algorithm_label = dict(ANALYSIS_MODES).get(analysis_algorithm, analysis_algorithm)

        asset_total = latest_import.assets.count()
        normalized_total = latest_import.conversations.count()

        is_normalizing = (
            latest_import.status == DatasetImport.Status.PROCESSING
            or latest_import.artifacts.filter(status=DatasetArtifact.Status.PROCESSING).exists()
        )
        can_normalize = (
            latest_import.artifacts.filter(status=DatasetArtifact.Status.DOWNLOADED).exists()
            and not is_normalizing
        )

        acquire_status = "pending"
        total_bytes_expected = latest_import.total_bytes or sum(
            row.get("size_bytes") or 0 for row in artifact_rows
        )
        if total_bytes_expected:
            download_progress_pct = int(
                min(100, (initial_downloaded_bytes / total_bytes_expected) * 100)
            )
        elif total_artifacts_display:
            download_progress_pct = int((downloaded_artifact_count / total_artifacts_display) * 100)

        if latest_import.status in {
            DatasetImport.Status.PENDING,
            DatasetImport.Status.QUEUED,
            DatasetImport.Status.DOWNLOADING,
        }:
            acquire_status = "in-progress"
        elif latest_import.status in {
            DatasetImport.Status.DOWNLOADED,
            DatasetImport.Status.ANALYZING,
            DatasetImport.Status.COMPLETED,
        }:
            acquire_status = "completed"
        elif latest_import.status in {
            DatasetImport.Status.FAILED,
            DatasetImport.Status.CANCELLED,
        }:
            acquire_status = "blocked"

        extract_status = "pending"
        if acquire_status in {"in-progress", "completed"} and asset_total == 0:
            extract_status = "in-progress"
        if asset_total > 0:
            extract_status = "completed"
        if latest_import.status == DatasetImport.Status.FAILED:
            extract_status = "blocked"

        normalize_status = "pending"
        if extract_status in {"in-progress", "completed"} and normalized_total == 0:
            normalize_status = "in-progress"
        if normalized_total > 0:
            normalize_status = "completed"
        if latest_import.status == DatasetImport.Status.FAILED:
            normalize_status = "blocked"

        analyze_status = "pending"
        if latest_import.analysis_status == "running" or latest_import.status == DatasetImport.Status.ANALYZING:
            analyze_status = "in-progress"
        elif latest_import.analysis_status == "completed" or latest_import.status == DatasetImport.Status.COMPLETED:
            analyze_status = "completed"
        elif latest_import.analysis_status == "failed" or latest_import.status == DatasetImport.Status.FAILED:
            analyze_status = "blocked"
        elif latest_import.analysis_status == "cancelled":
            analyze_status = "pending"

        artifact_total_for_pipeline = latest_import.total_artifacts or total_artifacts_defined
        pipeline_steps = [
            {
                "key": "acquire",
                "title": "Import",
                "description": "Download source artifacts from remote storage.",
                "status": acquire_status,
                "detail": (
                    f"{downloaded_artifact_count}/{artifact_total_for_pipeline} artifacts ready"
                    if artifact_total_for_pipeline
                    else "Waiting for downloads"
                ),
            },
            {
                "key": "extract",
                "title": "Extract",
                "description": "Prepare record-readable assets (e.g., uncompressed JSONL).",
                "status": extract_status,
                "detail": f"{asset_total} asset{'s' if asset_total != 1 else ''} available" if asset_total else "No assets yet",
            },
            {
                "key": "normalize",
                "title": "Normalize",
                "description": "Load records into the standardized dataset schema.",
                "status": normalize_status,
                "detail": f"{normalized_total} normalized conversation{'s' if normalized_total != 1 else ''}"
                if normalized_total
                else "Awaiting normalization",
            },
            {
                "key": "analyze",
                "title": "Analyze",
                "description": "Run downstream analysis for quality and summaries.",
                "status": analyze_status,
                "detail": analysis_algorithm_label or analysis_algorithm or "Not started",
            },
        ]
    else:
        pipeline_steps = [
            {
                "key": "acquire",
                "title": "Import",
                "description": "Download source artifacts from remote storage.",
                "status": "pending",
                "detail": "Waiting for first import",
            },
            {
                "key": "extract",
                "title": "Extract",
                "description": "Prepare record-readable assets (e.g., uncompressed JSONL).",
                "status": "pending",
                "detail": "No assets yet",
            },
            {
                "key": "normalize",
                "title": "Normalize",
                "description": "Load records into the standardized dataset schema.",
                "status": "pending",
                "detail": "Awaiting normalization",
            },
            {
                "key": "analyze",
                "title": "Analyze",
                "description": "Run downstream analysis for quality and summaries.",
                "status": "pending",
                "detail": "Not started",
            },
        ]

    pipeline_current_index: int | None = None
    for idx, step in enumerate(pipeline_steps):
        if step["status"] in {"in-progress", "blocked"}:
            pipeline_current_index = idx
            break
    if pipeline_current_index is None:
        for idx, step in enumerate(pipeline_steps):
            if step["status"] != "completed":
                pipeline_current_index = idx
                break
    if pipeline_current_index is None and pipeline_steps:
        pipeline_current_index = len(pipeline_steps) - 1

    for idx, step in enumerate(pipeline_steps):
        status = step["status"]
        segment_class = "usa-step-indicator__segment"
        if status == "completed":
            segment_class += " usa-step-indicator__segment--complete"
        elif status == "blocked":
            segment_class += " usa-step-indicator__segment--warning"
            if pipeline_current_index == idx:
                segment_class += " usa-step-indicator__segment--current"
        elif pipeline_current_index == idx:
            segment_class += " usa-step-indicator__segment--current"

        sr_label = {
            "completed": "completed",
            "in-progress": "in progress",
            "pending": "not completed",
            "blocked": "requires attention",
        }.get(status, "not completed")

        step["segment_class"] = segment_class
        step["sr_label"] = sr_label
        step["is_current"] = pipeline_current_index == idx

    pipeline_total_steps = len(pipeline_steps)
    pipeline_current_step = (pipeline_current_index + 1) if pipeline_current_index is not None else 0
    pipeline_current_title = (
        pipeline_steps[pipeline_current_index]["title"]
        if pipeline_current_index is not None and pipeline_steps
        else ""
    )

    context = {
        "dataset": dataset,
        "latest_import": latest_import,
        "artifact_rows": artifact_rows,
        "status_label": import_status_meta["label"],
        "status_badge_class": import_status_meta["badge_class"],
        "status_show_check": import_status_meta["show_check"],
        "can_launch": can_launch,
        "dataset_completed": dataset_completed,
        "analysis_modes": ANALYSIS_MODES,
        "selected_analysis_mode": latest_analysis_mode or DEFAULT_ANALYSIS_MODE,
        "selected_artifacts": selected_artifacts,
        "total_artifacts_defined": total_artifacts_defined,
        "selected_artifact_limit": latest_artifact_limit or total_artifacts_defined,
        "artifact_limit_options": list(range(1, total_artifacts_defined + 1)) if total_artifacts_defined else [],
        "analysis_mode_labels": analysis_mode_labels,
        "latest_analysis_label": latest_analysis_label,

        # Analysis controls
        "can_analyze": can_analyze,
        "is_analyzing": is_analyzing,
        "analysis_selection_enabled": analysis_selection_enabled,
        "analysis_cluster_count": analysis_cluster_count,
        "analysis_doc_limit": analysis_doc_limit,
        "default_cluster_count": DEFAULT_CLUSTER_COUNT,
        "downloaded_artifact_count": downloaded_artifact_count,
        "normalized_artifact_count": normalized_artifact_count,
        "normalized_artifacts": normalized_artifact_count,
        "download_progress_pct": download_progress_pct,
        "total_artifacts_display": total_artifacts_display,
        "initial_downloaded_bytes": initial_downloaded_bytes,
        "downloaded_bytes_total": downloaded_bytes_total,
        "total_bytes_expected": total_bytes_expected,
        "can_normalize": can_normalize,
        "is_normalizing": is_normalizing,
        "can_edit_artifacts": (can_launch or can_normalize),
        "status_stream_url": reverse("dataset_imports:status-stream", args=[dataset.id]),
        "normalize_url": reverse("dataset_imports:normalize", args=[dataset.id]),
        "analysis_summary": analysis_summary,
        "analysis_result": getattr(latest_import, "analysis_result", None) if latest_import else None,
        "analysis_algorithm": analysis_algorithm,
        "analysis_algorithm_label": analysis_algorithm_label or analysis_algorithm,
        "pipeline_total_steps": pipeline_total_steps,
        "pipeline_current_step": pipeline_current_step,
        "pipeline_current_title": pipeline_current_title,
        "pipeline_steps": pipeline_steps,
    }
    return render(request, "dataset_imports/detail.html", context)


def _build_artifact_rows(
    dataset: DatasetDefinition,
    latest_import: DatasetImport | None,
    *,
    selected_artifacts: Iterable[str] | None = None,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    artifact_map = {}
    artifact_limit = None
    analysis_events: list[dict[str, object]] = []
    analysis_events_json = "[]"
    selected_lookup: set[str] | None = None
    if selected_artifacts is not None:
        selected_lookup = {
            str(name).strip() for name in selected_artifacts if str(name).strip()
        }
    if latest_import:
        artifact_map = {
            artifact.filename: artifact for artifact in latest_import.artifacts.all()
        }
        if isinstance(latest_import.notes, dict):
            artifact_limit = latest_import.notes.get("artifact_limit")
            if selected_lookup is None:
                selected_lookup = {
                    str(name).strip()
                    for name in latest_import.notes.get("selected_artifacts", [])
                    if str(name).strip()
                }

        analysis_events = [
            {
                "label": str(DatasetImportEvent.Type(event.event_type).label),
                "message": event.message or "",
                "timestamp": event.created_at.isoformat(),
                "details": event.details or {},
            }
            for event in latest_import.events.all()
            if event.event_type == DatasetImportEvent.Type.ANALYSIS_COMPLETE
        ]
        analysis_events_json = json.dumps(analysis_events)

    for index, artifact_def in enumerate(dataset.artifacts, start=1):
        filename = artifact_def.filename
        matching = artifact_map.get(filename)
        if matching:
            status_value = matching.status
        else:
            if artifact_limit and index > artifact_limit:
                status_value = "not-requested"
            else:
                status_value = DatasetArtifact.Status.PENDING
        status_meta = _get_status_metadata(status_value)

        preview_data = _format_preview_for_display(
            matching.metadata.get("preview") if matching and isinstance(matching.metadata, dict) else None
        )
        suffix = Path(artifact_def.filename).suffix
        extension = suffix.lstrip(".") if suffix else ""

        downloaded_bytes = matching.downloaded_bytes if matching else 0
        size_bytes = matching.size_bytes if matching else 0
        download_progress_pct = None
        if size_bytes and size_bytes > 0:
            download_progress_pct = int(min(100, (downloaded_bytes / size_bytes) * 100))

        is_selected = True if selected_lookup is None else filename in selected_lookup

        rows.append(
            {
                "sequence": index,
                "definition": artifact_def,
                "status": status_value,
                "status_label": status_meta["label"],
                "status_badge_class": status_meta["badge_class"],
                "status_show_check": status_meta["show_check"],
                "is_selected": is_selected,
                "downloaded_bytes": downloaded_bytes,
                "processed_bytes": matching.processed_bytes if matching else 0,
                "size_bytes": size_bytes,
                "download_progress_pct": download_progress_pct,
                "checksum": matching.checksum if matching else "",
                "content_type": matching.content_type if matching else "",
                "finished_at": matching.finished_at.isoformat() if matching and matching.finished_at else None,
                "file_url": (
                    matching.file.url
                    if matching and matching.file and hasattr(matching.file, "url")
                    else None
                ),
                "extension": extension,
                "preview": preview_data,
                "metadata_items": (
                    [
                        {"key": key, "value": str(value)}
                        for key, value in matching.metadata.items()
                        if key
                        not in {"notes", "hf_token", "preview"}
                        and value not in (None, "")
                    ]
                    if matching and isinstance(matching.metadata, dict)
                    else []
                ),
                "processed_events": analysis_events,
                "processed_event_count": len(analysis_events),
                "processed_events_json": analysis_events_json,
            }
        )
    return rows


def _create_import_for_dataset(
    user_id: int | None,
    dataset: DatasetDefinition,
    *,
    analysis_mode: str | None = None,
    artifact_limit: int | None = None,
    cluster_count: int | None = None,
    selected_artifacts: list[str] | None = None,
) -> DatasetImport:
    timestamp = timezone.now().strftime("%Y%m%d%H%M%S")
    slug = slugify(f"{dataset.id}-{timestamp}")

    if not dataset.adapter:
        raise ValueError(f"Dataset {dataset.id} does not declare an adapter")

    normalized_analysis_mode = analysis_mode or DEFAULT_ANALYSIS_MODE
    valid_modes = {mode for mode, _ in ANALYSIS_MODES}
    if normalized_analysis_mode not in valid_modes:
        normalized_analysis_mode = DEFAULT_ANALYSIS_MODE

    defined_filenames = [artifact.filename for artifact in dataset.artifacts]
    defined_lookup = {artifact.filename: artifact for artifact in dataset.artifacts}

    selected_ordered: list[str] = []
    if selected_artifacts:
        candidates = []
        seen = set()
        for value in selected_artifacts:
            filename = str(value).strip()
            if filename in defined_lookup and filename not in seen:
                candidates.append(filename)
                seen.add(filename)
        if candidates:
            selected_set = set(candidates)
            selected_ordered = [name for name in defined_filenames if name in selected_set]

    if not selected_ordered:
        total_defined = len(defined_filenames)
        selected_limit = artifact_limit if artifact_limit is not None else total_defined
        if total_defined:
            selected_limit = max(1, min(selected_limit, total_defined))
        else:
            selected_limit = 0
        selected_ordered = defined_filenames[:selected_limit]
    else:
        selected_limit = len(selected_ordered)

    if not selected_ordered:
        raise ValueError("At least one artifact must be selected for import")

    cluster_value = cluster_count if cluster_count is not None else DEFAULT_CLUSTER_COUNT
    try:
        cluster_value = int(cluster_value)
    except (TypeError, ValueError):
        cluster_value = DEFAULT_CLUSTER_COUNT
    cluster_value = max(2, min(cluster_value, 50))

    import_obj = DatasetImport.objects.create(
        slug=slug,
        display_name=dataset.name,
        description=dataset.description,
        source_url=dataset.source_url,
        license=dataset.license,
        adapter=dataset.adapter,
        notes={
            "dataset_id": dataset.id,
            "dataset_name": dataset.name,
            "dataset_notes": dataset.notes,
            "analysis_mode": normalized_analysis_mode,
            "analysis_cluster_count": cluster_value,
            "artifact_limit": len(selected_ordered),
            "available_artifacts": defined_filenames,
            "selected_artifacts": selected_ordered,
        },
        created_by_id=user_id,
    )

    artifacts = []
    for sequence, filename in enumerate(selected_ordered):
        artifact_def = defined_lookup[filename]
        artifacts.append(
            DatasetArtifact(
                dataset_import=import_obj,
                sequence=sequence,
                filename=artifact_def.filename,
                download_url=artifact_def.download_url,
                status=DatasetArtifact.Status.PENDING,
                metadata=_build_artifact_metadata(artifact_def),
            )
        )
    DatasetArtifact.objects.bulk_create(artifacts)

    return import_obj


def _build_artifact_metadata(artifact_def: ArtifactDefinition) -> Dict[str, object]:
    metadata: Dict[str, object] = {}
    if artifact_def.metadata:
        metadata.update(artifact_def.metadata)
    if artifact_def.notes:
        metadata.setdefault("notes", artifact_def.notes)
    hf_token = os.environ.get("HF_TOKEN")
    if hf_token and not metadata.get("hf_token"):
        metadata["hf_token"] = hf_token
    return metadata


def _format_preview_for_display(preview: Any) -> Dict[str, object]:
    if not isinstance(preview, dict):
        return {}

    payload: Dict[str, object] = {}
    row_count = preview.get("row_count")
    if row_count is not None:
        try:
            payload["row_count"] = int(row_count)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            payload["row_count"] = row_count

    sample = preview.get("sample_row")
    if sample:
        payload["sample_row"] = str(sample)

    note = preview.get("note")
    if note:
        payload["note"] = str(note)

    error = preview.get("error")
    if error:
        payload["error"] = str(error)

    return payload


def _serialize_import_state(
    dataset: DatasetDefinition,
    latest_import: DatasetImport | None,
) -> Dict[str, object]:
    pipeline_current_index: int | None = None
    pipeline_steps: list[dict[str, object]] = [
        {
            "key": "acquire",
            "title": "Acquire",
            "description": "Download source artifacts from remote storage.",
            "status": "pending",
            "detail": "Waiting for first import",
        },
        {
            "key": "extract",
            "title": "Extract",
            "description": "Prepare record-readable assets (e.g., uncompressed JSONL).",
            "status": "pending",
            "detail": "No assets yet",
        },
        {
            "key": "normalize",
            "title": "Normalize",
            "description": "Load records into the standardized dataset schema.",
            "status": "pending",
            "detail": "Awaiting normalization",
        },
        {
            "key": "analyze",
            "title": "Analyze",
            "description": "Run downstream analysis for quality and summaries.",
            "status": "pending",
            "detail": "Not started",
        },
    ]

    artifact_rows = _build_artifact_rows(dataset, latest_import)
    artifacts_payload = [
        {
            "sequence": row["sequence"],
            "filename": row["definition"].filename,
            "notes": row["definition"].notes,
            "status": row["status"],
            "status_label": row["status_label"],
            "status_badge_class": row["status_badge_class"],
            "status_show_check": row["status_show_check"],
            "is_selected": row.get("is_selected", True),
            "downloaded_bytes": row["downloaded_bytes"],
            "processed_bytes": row["processed_bytes"],
            "checksum": row.get("checksum"),
            "content_type": row.get("content_type"),
            "finished_at": row.get("finished_at"),
            "file_url": row.get("file_url"),
            "extension": row.get("extension"),
            "size_bytes": row.get("size_bytes"),
            "download_progress_pct": row.get("download_progress_pct"),
            "metadata_items": row.get("metadata_items", []),
            "preview": row.get("preview", {}),
        }
        for row in artifact_rows
    ]

    # Sum preview row counts to estimate total messages expected for the active selection
    expected_message_total = 0
    for _art in artifacts_payload:
        if not _art.get("is_selected", True):
            continue
        preview = _art.get("preview") or {}
        try:
            rc = int(preview.get("row_count")) if preview.get("row_count") is not None else 0
        except (TypeError, ValueError):
            rc = 0
        if rc > 0:
            expected_message_total += rc

    selected_artifacts_payload = [
        artifact["filename"]
        for artifact in artifacts_payload
        if artifact.get("is_selected", True)
    ]

    can_launch_state = dataset.supported and (
        latest_import is None
        or latest_import.status
        in {
            DatasetImport.Status.FAILED,
            DatasetImport.Status.CANCELLED,
        }
    )

    completed_download_states = {
        DatasetArtifact.Status.DOWNLOADED,
        DatasetArtifact.Status.COMPLETED,
        DatasetArtifact.Status.FAILED,
    }
    download_phase_complete = False
    has_download_failures = False
    if latest_import:
        artifact_statuses = list(
            latest_import.artifacts.values_list("status", flat=True)
        )
        if artifact_statuses:
            download_phase_complete = all(
                status in completed_download_states for status in artifact_statuses
            )
            has_download_failures = (DatasetArtifact.Status.FAILED in artifact_statuses)

    pipeline_current_index: int | None = None

    if not latest_import:
        for idx, step in enumerate(pipeline_steps):
            segment_class = "usa-step-indicator__segment"
            if idx == 0:
                segment_class += " usa-step-indicator__segment--current"
                pipeline_current_index = 0
            step["segment_class"] = segment_class
            step["sr_label"] = "not completed"
            step["is_current"] = pipeline_current_index == idx

        pipeline_total_steps = len(pipeline_steps)
        pipeline_current_step = (pipeline_current_index + 1) if pipeline_current_index is not None else 0
        pipeline_current_title = pipeline_steps[0]["title"] if pipeline_steps else ""

        metadata = _get_status_metadata(None)
        return {
            "dataset_id": dataset.id,
            "status": "not-started",
            "status_label": metadata["label"],
            "status_badge_class": metadata["badge_class"],
            "status_show_check": metadata["show_check"],
            "started_at": None,
            "updated_at": None,
            "downloaded_bytes": 0,
            "total_bytes_expected": 0,
            "processed_bytes": 0,
            "processed_artifacts": 0,
            "running_normalization_tasks": 0,
            "normalized_artifact_count": 0,
            "normalized_conversation_count": 0,
            "normalized_message_count": 0,
            "normalized_artifacts": 0,
            "total_artifacts": len(artifacts_payload),
            "download_progress": 0,
            "analysis_mode": None,
            "analysis_cluster_count": DEFAULT_CLUSTER_COUNT,
            "artifact_limit": len(selected_artifacts_payload) or None,
            "selected_artifacts": selected_artifacts_payload,
            "analysis_status": "",
            "analysis_progress": 0,
            "analysis_message": "",
            "analysis_started_at": None,
            "analysis_finished_at": None,
            "can_analyze": False,
            "is_analyzing": False,
            "can_normalize": False,
            "is_normalizing": False,
            "normalized_artifacts": 0,
            "artifacts": artifacts_payload,
            "downloaded_artifacts": 0,
            "normalized_message_expected": expected_message_total,
            "analysis_summary": None,
            "analysis_jobs": [],
            "analysis_algorithm": None,
            "analysis_result_id": None,
            "pipeline_steps": pipeline_steps,
            "pipeline_total_steps": pipeline_total_steps,
            "pipeline_current_step": pipeline_current_step,
            "pipeline_current_title": pipeline_current_title,
            "can_launch": can_launch_state,
        }

    if (
        download_phase_complete
        and latest_import.status
        in {
            DatasetImport.Status.PENDING,
            DatasetImport.Status.QUEUED,
            DatasetImport.Status.DOWNLOADING,
        }
    ):
        reconcile_import_status(
            latest_import,
            download_complete=True,
            has_failures=has_download_failures,
        )

    import_status_metadata = _get_status_metadata(latest_import.status)
    notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}

    downloaded_artifacts = len(
        [
            artifact
            for artifact in artifacts_payload
            if artifact["status"]
            in (DatasetArtifact.Status.DOWNLOADED, DatasetArtifact.Status.COMPLETED)
        ]
    )
    total_artifacts = latest_import.total_artifacts or len(artifacts_payload)
    download_progress = 0

    downloaded_bytes_total = sum(row["downloaded_bytes"] for row in artifacts_payload)
    processed_bytes_total = sum(row["processed_bytes"] for row in artifacts_payload)
    total_bytes_expected = latest_import.total_bytes or sum(
        row.get("size_bytes") or 0 for row in artifacts_payload
    )
    if total_bytes_expected:
        download_progress = int(
            min(100, (downloaded_bytes_total / total_bytes_expected) * 100)
        )
    elif total_artifacts:
        download_progress = int((downloaded_artifacts / total_artifacts) * 100)

    download_progress_pct = download_progress

    status_badge_class = import_status_metadata["badge_class"]
    status_show_check = import_status_metadata["show_check"]
    status_label = import_status_metadata["label"]
    if latest_import.status == DatasetImport.Status.QUEUED and downloaded_bytes_total == 0:
        status_label = "Queued"
        status_badge_class = "status-tag--neutral"
        status_show_check = False

    analysis_result = getattr(latest_import, "analysis_result", None)
    analysis_summary = notes.get("analysis_summary")
    if analysis_result and not analysis_summary:
        analysis_summary = analysis_result.summary
    analysis_algorithm = None
    if analysis_result and analysis_result.algorithm:
        analysis_algorithm = analysis_result.algorithm
    elif notes.get("analysis_mode"):
        analysis_algorithm = notes.get("analysis_mode")
    analysis_algorithm_label = None
    if analysis_algorithm:
        analysis_algorithm_label = dict(ANALYSIS_MODES).get(analysis_algorithm, analysis_algorithm)

    normalized_artifacts = latest_import.artifacts.filter(
        status=DatasetArtifact.Status.COMPLETED
    ).count()
    running_normalization_tasks = latest_import.artifacts.filter(
        status=DatasetArtifact.Status.PROCESSING
    ).count()
    is_normalizing = (
        latest_import.status == DatasetImport.Status.PROCESSING
        or latest_import.artifacts.filter(status=DatasetArtifact.Status.PROCESSING).exists()
    )
    pending_normalization = latest_import.artifacts.filter(
        status=DatasetArtifact.Status.DOWNLOADED
    ).exists()
    can_normalize = pending_normalization and not is_normalizing

    # Aggregate row totals for clarity in the UI
    normalized_conversations_total = latest_import.conversations.count()
    try:
        normalized_messages_total = MessageRecord.objects.filter(
            conversation__dataset_import=latest_import
        ).count()
    except Exception:  # pragma: no cover
        normalized_messages_total = 0

    jobs_payload: list[dict[str, object]] = []
    for job in latest_import.analysis_jobs.order_by("-started_at")[:10]:
        jobs_payload.append(
            {
                "id": job.pk,
                "status": job.status,
                "status_label": job.get_status_display(),
                "requested_mode": job.requested_mode,
                "requested_cluster_count": job.requested_cluster_count,
                "algorithm": job.algorithm,
                "cluster_count": job.cluster_count,
                "document_count": job.document_count,
                "task_id": job.task_id,
                "error": job.error,
                "metadata": job.metadata,
                "started_at": job.started_at.isoformat(),
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            }
        )

    asset_total = latest_import.assets.count()
    normalized_total = latest_import.conversations.count()

    acquire_status = "pending"
    if latest_import.status in {
        DatasetImport.Status.PENDING,
        DatasetImport.Status.QUEUED,
        DatasetImport.Status.DOWNLOADING,
    }:
        acquire_status = "in-progress"
    elif latest_import.status in {
        DatasetImport.Status.DOWNLOADED,
        DatasetImport.Status.ANALYZING,
        DatasetImport.Status.COMPLETED,
    }:
        acquire_status = "completed"
    elif latest_import.status in {
        DatasetImport.Status.FAILED,
        DatasetImport.Status.CANCELLED,
    }:
        acquire_status = "blocked"

    extract_status = "pending"
    if acquire_status in {"in-progress", "completed"} and asset_total == 0:
        extract_status = "in-progress"
    if asset_total > 0:
        extract_status = "completed"
    if latest_import.status == DatasetImport.Status.FAILED:
        extract_status = "blocked"

    normalize_status = "pending"
    if extract_status in {"in-progress", "completed"} and normalized_total == 0:
        normalize_status = "in-progress"
    if normalized_total > 0:
        normalize_status = "completed"
    if latest_import.status == DatasetImport.Status.FAILED:
        normalize_status = "blocked"

    analyze_status = "pending"
    if latest_import.analysis_status == "running" or latest_import.status == DatasetImport.Status.ANALYZING:
        analyze_status = "in-progress"
    elif latest_import.analysis_status == "completed" or latest_import.status == DatasetImport.Status.COMPLETED:
        analyze_status = "completed"
    elif latest_import.analysis_status == "failed" or latest_import.status == DatasetImport.Status.FAILED:
        analyze_status = "blocked"
    elif latest_import.analysis_status == "cancelled":
        analyze_status = "pending"

    artifact_total_for_pipeline = total_artifacts
    if not artifact_total_for_pipeline:
        artifact_total_for_pipeline = len(artifacts_payload)

    pipeline_steps = [
        {
            "key": "acquire",
            "title": "Acquire",
            "description": "Download source artifacts from remote storage.",
            "status": acquire_status,
            "detail": (
                f"{downloaded_artifacts}/{artifact_total_for_pipeline} artifacts ready"
                if artifact_total_for_pipeline
                else "Waiting for downloads"
            ),
        },
        {
            "key": "extract",
            "title": "Extract",
            "description": "Prepare record-readable assets (e.g., uncompressed JSONL).",
            "status": extract_status,
            "detail": f"{asset_total} asset{'s' if asset_total != 1 else ''} available" if asset_total else "No assets yet",
        },
        {
            "key": "normalize",
            "title": "Normalize",
            "description": "Load records into the standardized dataset schema.",
            "status": normalize_status,
            "detail": f"{normalized_total} normalized conversation{'s' if normalized_total != 1 else ''}" if normalized_total else "Awaiting normalization",
        },
        {
            "key": "analyze",
            "title": "Analyze",
            "description": "Run downstream analysis for quality and summaries.",
            "status": analyze_status,
            "detail": analysis_algorithm_label or analysis_algorithm or "Not started",
        },
    ]

    pipeline_current_index = None
    for idx, step in enumerate(pipeline_steps):
        if step["status"] in {"in-progress", "blocked"}:
            pipeline_current_index = idx
            break
    if pipeline_current_index is None:
        for idx, step in enumerate(pipeline_steps):
            if step["status"] != "completed":
                pipeline_current_index = idx
                break
    if pipeline_current_index is None and pipeline_steps:
        pipeline_current_index = len(pipeline_steps) - 1

    for idx, step in enumerate(pipeline_steps):
        status = step["status"]
        segment_class = "usa-step-indicator__segment"
        if status == "completed":
            segment_class += " usa-step-indicator__segment--complete"
        elif status == "blocked":
            segment_class += " usa-step-indicator__segment--warning"
            if pipeline_current_index == idx:
                segment_class += " usa-step-indicator__segment--current"
        elif pipeline_current_index == idx:
            segment_class += " usa-step-indicator__segment--current"

        sr_label = {
            "completed": "completed",
            "in-progress": "in progress",
            "pending": "not completed",
            "blocked": "requires attention",
        }.get(status, "not completed")

        step["segment_class"] = segment_class
        step["sr_label"] = sr_label
        step["is_current"] = pipeline_current_index == idx

    pipeline_total_steps = len(pipeline_steps)
    pipeline_current_step = (pipeline_current_index + 1) if pipeline_current_index is not None else 0
    pipeline_current_title = (
        pipeline_steps[pipeline_current_index]["title"]
        if pipeline_current_index is not None and pipeline_steps
        else ""
    )

    return {
        "dataset_id": dataset.id,
        "status": latest_import.status,
        "status_label": status_label,
        "status_badge_class": status_badge_class,
        "status_show_check": status_show_check,
        "started_at": latest_import.started_at.isoformat() if latest_import.started_at else None,
        "updated_at": latest_import.updated_at.isoformat() if latest_import.updated_at else None,
        "downloaded_bytes": downloaded_bytes_total,
        "processed_bytes": processed_bytes_total,
        "processed_artifacts": downloaded_artifacts,
        "total_artifacts": total_artifacts,
        "download_progress": download_progress,
        "analysis_mode": notes.get("analysis_mode"),
        "analysis_cluster_count": notes.get("analysis_cluster_count", DEFAULT_CLUSTER_COUNT),
        "analysis_cluster_doc_limit": notes.get("analysis_cluster_doc_limit", 1500),
        "artifact_limit": notes.get("artifact_limit"),
        "selected_artifacts": selected_artifacts_payload,
        "total_bytes_expected": total_bytes_expected,
        "artifacts": artifacts_payload,
        "downloaded_artifacts": downloaded_artifacts,
        "running_normalization_tasks": running_normalization_tasks,
        "normalized_conversation_count": normalized_conversations_total,
        "normalized_message_count": normalized_messages_total,
        "normalized_message_expected": expected_message_total,
        "analysis_status": latest_import.analysis_status or "",
        "analysis_progress": latest_import.analysis_progress or 0,
        "analysis_message": latest_import.analysis_message or "",
        "analysis_started_at": latest_import.analysis_started_at.isoformat()
        if latest_import.analysis_started_at
        else None,
        "analysis_finished_at": latest_import.analysis_finished_at.isoformat()
        if latest_import.analysis_finished_at
        else None,
        "can_analyze": (
            latest_import.status
            in {
                DatasetImport.Status.DOWNLOADED,
                DatasetImport.Status.COMPLETED,
            }
            and normalized_artifacts > 0
        ),
        "is_analyzing": latest_import.status == DatasetImport.Status.ANALYZING,
        "can_normalize": can_normalize,
        "is_normalizing": is_normalizing,
        "normalized_artifacts": normalized_artifacts,
        "analysis_summary": analysis_summary,
        "analysis_jobs": jobs_payload,
        "analysis_algorithm": analysis_algorithm,
        "analysis_algorithm_label": analysis_algorithm_label,
        "analysis_result_id": analysis_result.id if analysis_result else None,
        "pipeline_steps": pipeline_steps,
        "pipeline_total_steps": pipeline_total_steps,
        "pipeline_current_step": pipeline_current_step,
        "pipeline_current_title": pipeline_current_title,
        "normalize_url": reverse("dataset_imports:normalize", args=[dataset.id]),
        "can_launch": can_launch_state,
    }


def status_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        raise Http404("Unknown dataset")

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )
    payload = _serialize_import_state(dataset, latest_import)
    return JsonResponse(payload)


def status_stream(request: HttpRequest, dataset_id: str) -> StreamingHttpResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        raise Http404("Unknown dataset")

    HEARTBEAT_INTERVAL = 30

    def event_stream():
        last_payload: Dict[str, object] | None = None
        idle_ticks = 0

        while True:
            try:
                latest_import = (
                    DatasetImport.objects.filter(notes__dataset_id=dataset.id)
                    .prefetch_related("artifacts")
                    .order_by("-created_at")
                    .first()
                )

                payload = _serialize_import_state(dataset, latest_import)

                if payload != last_payload:
                    last_payload = payload
                    idle_ticks = 0
                    yield f"data: {json.dumps(payload)}\n\n"
                else:
                    idle_ticks += 1
                    if idle_ticks >= HEARTBEAT_INTERVAL:
                        idle_ticks = 0
                        yield ": heartbeat\n\n"

                time.sleep(1)
            except GeneratorExit:
                break

    response = StreamingHttpResponse(event_stream(), content_type="text/event-stream")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@require_POST
def start_import_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not dataset.supported:
        return JsonResponse({"error": "Ingestion pipeline not yet configured for this dataset."}, status=400)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can launch dataset imports."}, status=403)

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .order_by("-created_at")
        .first()
    )
    if latest_import and latest_import.status not in {
        DatasetImport.Status.COMPLETED,
        DatasetImport.Status.FAILED,
        DatasetImport.Status.CANCELLED,
        DatasetImport.Status.DOWNLOADED,
    }:
        payload = _serialize_import_state(dataset, latest_import)
        payload.update({"error": "An import is already running for this dataset."})
        return JsonResponse(payload, status=409)

    try:
        data = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON payload."}, status=400)

    requested_analysis_mode = (data.get("analysis_mode") or "").strip().lower()
    valid_modes = {mode for mode, _ in ANALYSIS_MODES}
    if requested_analysis_mode not in valid_modes:
        requested_analysis_mode = DEFAULT_ANALYSIS_MODE

    total_artifacts_defined = len(dataset.artifacts)
    artifact_limit_value = data.get("artifact_limit")
    if artifact_limit_value is None:
        artifact_limit = total_artifacts_defined
    else:
        try:
            artifact_limit = int(artifact_limit_value)
        except (TypeError, ValueError):
            artifact_limit = total_artifacts_defined
    if total_artifacts_defined:
        artifact_limit = max(1, min(artifact_limit, total_artifacts_defined))
    else:
        artifact_limit = 0

    cluster_raw = data.get("cluster_count")
    try:
        api_cluster_count = int(cluster_raw) if cluster_raw is not None else DEFAULT_CLUSTER_COUNT
    except (TypeError, ValueError):
        api_cluster_count = DEFAULT_CLUSTER_COUNT
    api_cluster_count = max(2, min(api_cluster_count, 50))

    with transaction.atomic():
        import_obj = _create_import_for_dataset(
            request.user.id,
            dataset,
            analysis_mode=requested_analysis_mode,
            artifact_limit=artifact_limit,
            cluster_count=api_cluster_count,
        )
        sync_import_aggregates(import_obj)

    message = "Import queued. Background workers will begin downloading shortly."
    status_code = 200
    try:
        run_dataset_import.delay(import_obj.pk)
    except Exception as exc:  # pragma: no cover - celery misconfiguration
        message = f"Import created but Celery dispatch failed: {exc}"
        import_obj.last_error = str(exc)
        import_obj.status = DatasetImport.Status.FAILED
        import_obj.save(update_fields=["last_error", "status", "updated_at"])
        status_code = 500

    payload = _serialize_import_state(dataset, import_obj)
    payload.update({"message": message})
    return JsonResponse(payload, status=status_code)


@require_POST
def reset_import_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can reset imports."}, status=403)

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts", "assets")
        .order_by("-created_at")
        .first()
    )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"message": "No existing import to reset."})
        return JsonResponse(payload)

    with transaction.atomic():
        for asset in latest_import.assets.all():
            if asset.file:
                asset.file.delete(save=False)
        for artifact in latest_import.artifacts.all():
            if artifact.file:
                artifact.file.delete(save=False)
        latest_import.delete()

    payload = _serialize_import_state(dataset, None)
    payload.update({"message": "Ingestion state reset."})
    return JsonResponse(payload)


@require_POST
def start_normalization_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can start normalization."}, status=403)

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"error": "No import found for this dataset."})
        return JsonResponse(payload, status=400)

    if latest_import.status in {
        DatasetImport.Status.PENDING,
        DatasetImport.Status.QUEUED,
        DatasetImport.Status.DOWNLOADING,
    }:
        payload = _serialize_import_state(dataset, latest_import)
        payload.update({"error": "Finish downloading artifacts before starting normalization."})
        return JsonResponse(payload, status=400)

    # Optional payload: allow selecting a subset of artifacts to normalize
    selected_lookup: set[str] | None = None
    try:
        body = json.loads(request.body or b"{}")
        if isinstance(body, dict) and body.get("selected_artifacts"):
            selected_lookup = {
                str(name).strip()
                for name in (body.get("selected_artifacts") or [])
                if str(name).strip()
            }
            if not selected_lookup:
                selected_lookup = None
    except json.JSONDecodeError:
        selected_lookup = None

    artifacts_to_queue = []
    waiting_downloads = 0
    for artifact in latest_import.artifacts.order_by("sequence", "id"):
        # If a selection was provided, only consider matching artifacts
        if selected_lookup is not None and artifact.filename not in selected_lookup:
            continue
        if artifact.status == DatasetArtifact.Status.COMPLETED:
            continue
        if artifact.status == DatasetArtifact.Status.DOWNLOADED:
            artifacts_to_queue.append(artifact)
        elif artifact.status == DatasetArtifact.Status.FAILED and artifact.file:
            artifact.status = DatasetArtifact.Status.DOWNLOADED
            artifact.last_error = ""
            artifact.save(update_fields=["status", "last_error", "updated_at"])
            artifacts_to_queue.append(artifact)
        else:
            waiting_downloads += 1

    if not artifacts_to_queue:
        payload = _serialize_import_state(dataset, latest_import)
        if waiting_downloads:
            payload.update({"message": f"Waiting for {waiting_downloads} artifact{'s' if waiting_downloads != 1 else ''} to finish downloading before normalization can start."})
            status_code = 200
        else:
            payload.update({"message": "No downloaded artifacts awaiting normalization."})
            status_code = 200
        return JsonResponse(payload, status=status_code)

    # Optional: cap normalization to a maximum number of conversations
    normalize_limit = None
    try:
        body = json.loads(request.body or b"{}")
        if isinstance(body, dict) and body.get("normalize_limit"):
            try:
                normalize_limit = int(body.get("normalize_limit"))
            except (TypeError, ValueError):
                normalize_limit = None
    except json.JSONDecodeError:
        pass

    if normalize_limit and artifacts_to_queue:
        # Queue only the first artifact and record a per-artifact limit
        artifacts_to_queue = [artifacts_to_queue[0]]
        notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
        notes = dict(notes or {})
        raw_limits = notes.get("normalize_limits")
        limits: dict[str, int] = {}
        if isinstance(raw_limits, dict):
            for k, v in raw_limits.items():
                try:
                    iv = int(v)
                    if iv > 0:
                        limits[str(k)] = iv
                except Exception:
                    continue
        limits[str(artifacts_to_queue[0].pk)] = max(1, int(normalize_limit))
        notes["normalize_limits"] = limits
        latest_import.notes = notes
        latest_import.save(update_fields=["notes", "updated_at"])

    previous_status = latest_import.status
    latest_import.status = DatasetImport.Status.PROCESSING
    latest_import.save(update_fields=["status", "updated_at"])

    queued = 0
    task_map: dict[str, str] = {}
    # Initialize/clone notes for storing task ids
    notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
    notes = dict(notes or {})
    normalize_tasks: dict[str, str] = {}
    try:
        raw = notes.get("normalize_tasks")
        if isinstance(raw, dict):
            normalize_tasks = {str(k): str(v) for k, v in raw.items() if k and v}
    except Exception:
        normalize_tasks = {}
    for artifact in artifacts_to_queue:
        try:
            async_result = process_artifact.delay(artifact.pk)
            # Record Celery task id if available for later cancellation
            task_id = getattr(async_result, "id", None)
            if task_id:
                normalize_tasks[str(artifact.pk)] = str(task_id)
            queued += 1
        except Exception as exc:  # pragma: no cover - celery misconfiguration
            DatasetImportLog.objects.create(
                dataset_import=latest_import,
                level="ERROR",
                message="Normalization enqueue failed",
                extra={"artifact_id": artifact.pk, "filename": artifact.filename, "error": str(exc)},
            )

    if queued:
        DatasetImportLog.objects.create(
            dataset_import=latest_import,
            level="INFO",
            message="Normalization started",
            extra={"artifact_count": queued},
        )
    else:
        latest_import.status = previous_status
        latest_import.save(update_fields=["status", "updated_at"])

    # Persist updated task map, if any
    if normalize_tasks:
        notes["normalize_tasks"] = normalize_tasks
        latest_import.notes = notes
        latest_import.save(update_fields=["notes", "updated_at"])

    payload = _serialize_import_state(dataset, latest_import)
    payload.update({"message": f"Queued normalization for {queued} artifact{'s' if queued != 1 else ''}."})
    return JsonResponse(payload)


@require_POST
def start_analysis_view(request: HttpRequest, dataset_id: str) -> JsonResponse:
    """Start analysis for a completed dataset import."""
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can start analysis"}, status=403)

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"error": "No import found for this dataset"})
        return JsonResponse(payload, status=400)

    payload = _serialize_import_state(dataset, latest_import)
    if latest_import.status == DatasetImport.Status.ANALYZING:
        payload.update({"message": "Analysis already running"})
        return JsonResponse(payload, status=200)

    if latest_import.status not in {
        DatasetImport.Status.DOWNLOADED,
        DatasetImport.Status.COMPLETED,
    }:
        payload.update(
            {
                "error": (
                    "Import must be downloaded or completed before analysis. "
                    f"Current status: {latest_import.status}"
                )
            }
        )
        return JsonResponse(payload, status=400)

    if latest_import.analysis_status == "running":
        payload.update({"message": "Analysis already running"})
        return JsonResponse(payload, status=200)

    try:
        data = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        payload.update({"error": "Invalid JSON payload."})
        return JsonResponse(payload, status=400)

    if not latest_import.artifacts.filter(status=DatasetArtifact.Status.COMPLETED).exists():
        payload.update({"error": "Normalize downloaded artifacts before running analysis."})
        return JsonResponse(payload, status=400)

    requested_analysis_mode = (data.get("analysis_mode") or "").strip().lower()
    notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
    notes = dict(notes)  # ensure we operate on a mutable copy

    if requested_analysis_mode:
        valid_modes = {mode for mode, _ in ANALYSIS_MODES}
        if requested_analysis_mode not in valid_modes:
            payload.update({"error": "Unknown analysis approach."})
            return JsonResponse(payload, status=400)

        notes["analysis_mode"] = requested_analysis_mode
    elif notes.get("analysis_mode"):
        requested_analysis_mode = notes["analysis_mode"]
    else:
        requested_analysis_mode = DEFAULT_ANALYSIS_MODE

    cluster_value = notes.get("analysis_cluster_count")
    cluster_raw = data.get("cluster_count")
    if cluster_raw is not None:
        try:
            cluster_value = int(cluster_raw)
        except (TypeError, ValueError):
            cluster_value = DEFAULT_CLUSTER_COUNT
        cluster_value = max(2, min(cluster_value, 50))
        notes["analysis_cluster_count"] = cluster_value
    elif cluster_value is None:
        cluster_value = DEFAULT_CLUSTER_COUNT

    # Optional document limit for topic clustering
    doc_limit_value = notes.get("analysis_cluster_doc_limit")
    doc_limit_raw = data.get("document_limit")
    if doc_limit_raw is not None:
        try:
            doc_limit_value = int(doc_limit_raw)
        except (TypeError, ValueError):
            doc_limit_value = 1500
        doc_limit_value = max(100, min(doc_limit_value, 20000))
        notes["analysis_cluster_doc_limit"] = doc_limit_value
    elif doc_limit_value is None:
        doc_limit_value = 1500

    job = AnalysisJob.objects.create(
        dataset_import=latest_import,
        requested_mode=requested_analysis_mode,
        requested_cluster_count=cluster_value,
        status=AnalysisJob.Status.RUNNING,
    )

    notes["analysis_job_id"] = job.pk
    latest_import.notes = notes
    latest_import.save(update_fields=["notes", "updated_at"])

    # Start analysis
    try:
        async_result = run_dataset_analysis.delay(latest_import.pk, job_id=job.pk)
        latest_import.set_analysis_task_id(async_result.id)
        job.task_id = async_result.id
        job.save(update_fields=["task_id", "updated_at"])
    except Exception as exc:  # pragma: no cover - celery misconfiguration
        job.mark_failed(f"Dispatch error: {exc}")
        notes.pop("analysis_job_id", None)
        latest_import.notes = notes
        latest_import.save(update_fields=["notes", "updated_at"])
        latest_import.refresh_from_db()
        payload = _serialize_import_state(dataset, latest_import)
        payload.update({"error": f"Failed to start analysis: {exc}"})
        return JsonResponse(payload, status=500)

    latest_import.refresh_from_db()
    refreshed = (
        DatasetImport.objects.filter(pk=latest_import.pk)
        .prefetch_related("artifacts")
        .first()
    )
    response_payload = _serialize_import_state(dataset, refreshed)
    response_payload.update({"message": "Analysis started", "selected_analysis_mode": requested_analysis_mode})
    return JsonResponse(response_payload)


@require_POST
def reset_analysis_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    """Reset analysis state without deleting downloaded artifacts."""

    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can reset analysis."}, status=403)

    latest_import = (
                    DatasetImport.objects.filter(notes__dataset_id=dataset.id)
                    .select_related("analysis_result")
                    .prefetch_related("artifacts")
                    .order_by("-created_at")
                    .first()
                )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"message": "No import found to reset."})
        return JsonResponse(payload, status=400)

    if latest_import.status not in {
        DatasetImport.Status.ANALYZING,
        DatasetImport.Status.FAILED,
        DatasetImport.Status.COMPLETED,
        DatasetImport.Status.DOWNLOADED,
    }:
        payload = _serialize_import_state(dataset, latest_import)
        payload.update({"error": "Analysis reset is only available for downloaded or analyzed imports."})
        return JsonResponse(payload, status=400)

    latest_import.reset_analysis()

    running_job = (
        latest_import.analysis_jobs.filter(status=AnalysisJob.Status.RUNNING)
        .order_by("-started_at")
        .first()
    )
    if running_job:
        running_job.mark_cancelled("Analysis reset")

    notes = dict(latest_import.notes or {})
    notes.pop("analysis_job_id", None)
    latest_import.notes = notes
    latest_import.save(update_fields=["notes", "updated_at"])
    DatasetImportLog.objects.create(
        dataset_import=latest_import,
        level="INFO",
        message="Analysis state reset",
        extra={"requested_by": request.user.pk},
    )

    refreshed = (
        DatasetImport.objects.filter(pk=latest_import.pk)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .first()
    )
    payload = _serialize_import_state(dataset, refreshed)
    payload.update({"message": "Analysis state reset."})
    return JsonResponse(payload)


@require_POST
def cancel_analysis_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    """Cancel analysis and revert import status to downloaded."""

    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can cancel analysis."}, status=403)

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"message": "No import found to cancel."})
        return JsonResponse(payload, status=400)

    if latest_import.status != DatasetImport.Status.ANALYZING:
        payload = _serialize_import_state(dataset, latest_import)
        payload.update({"error": "Analysis is not running for this dataset."})
        return JsonResponse(payload, status=400)

    reason = None
    try:
        body = json.loads(request.body or b"{}")
        reason = body.get("reason") if isinstance(body, dict) else None
    except json.JSONDecodeError:
        pass

    task_id = None
    if isinstance(latest_import.notes, dict):
        task_id = latest_import.notes.get("analysis_task_id")

    latest_import.cancel_analysis(reason=reason)
    running_job = (
        latest_import.analysis_jobs.filter(status=AnalysisJob.Status.RUNNING)
        .order_by("-started_at")
        .first()
    )
    if running_job:
        running_job.mark_cancelled(reason or "Analysis cancelled")
    DatasetImportLog.objects.create(
        dataset_import=latest_import,
        level="INFO",
        message="Analysis cancelled",
        extra={
            "requested_by": request.user.pk,
            "reason": reason,
        },
    )
    log_dataset_event(
        latest_import,
        DatasetImportEvent.Type.ANALYSIS_CANCEL,
        details={
            "requested_by": request.user.pk,
            "reason": reason,
        },
    )

    if task_id:
        try:
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        except Exception as exc:  # pragma: no cover - best effort logging
            _extra = {"task_id": task_id, "error": str(exc)}
            DatasetImportLog.objects.create(
                dataset_import=latest_import,
                level="ERROR",
                message="Failed to revoke analysis task",
                extra=_extra,
            )

    refreshed = (
        DatasetImport.objects.filter(pk=latest_import.pk)
        .select_related("analysis_result")
        .prefetch_related("artifacts")
        .first()
    )
    payload = _serialize_import_state(dataset, refreshed)
    payload.update({"message": "Analysis cancelled."})
    return JsonResponse(payload)


@require_POST
def cancel_normalization_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    """Stop in-flight normalization tasks and revert artifacts to downloaded.

    Best effort: attempts to revoke Celery tasks recorded during enqueue and
    resets artifact statuses from processing->downloaded. Already completed
    artifacts remain completed.
    """

    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        return JsonResponse({"error": "Dataset not found"}, status=404)

    if not request.user.is_staff:
        return JsonResponse({"error": "Only staff users can cancel normalization."}, status=403)

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"message": "No import found to cancel normalization."})
        return JsonResponse(payload, status=400)

    # Revoke any recorded task ids
    task_map = {}
    if isinstance(latest_import.notes, dict):
        raw = latest_import.notes.get("normalize_tasks")
        if isinstance(raw, dict):
            task_map = {str(k): str(v) for k, v in raw.items() if k and v}

    for _artifact_id, task_id in task_map.items():
        try:
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        except Exception as exc:  # pragma: no cover
            DatasetImportLog.objects.create(
                dataset_import=latest_import,
                level="ERROR",
                message="Failed to revoke normalization task",
                extra={"task_id": task_id, "error": str(exc)},
            )

    # Reset in-progress artifacts back to downloaded so user can re-run
    in_progress = latest_import.artifacts.filter(status=DatasetArtifact.Status.PROCESSING)
    for art in in_progress:
        art.status = DatasetArtifact.Status.DOWNLOADED
        art.last_error = ""
        art.processed_bytes = 0
        art.save(update_fields=["status", "last_error", "processed_bytes", "updated_at"])

    # Clear normalize task map
    notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
    notes = dict(notes or {})
    notes.pop("normalize_tasks", None)
    latest_import.notes = notes
    latest_import.status = DatasetImport.Status.DOWNLOADED
    latest_import.save(update_fields=["notes", "status", "updated_at"])

    DatasetImportLog.objects.create(
        dataset_import=latest_import,
        level="INFO",
        message="Normalization cancelled",
        extra={"requested_by": request.user.pk},
    )

    refreshed = (
        DatasetImport.objects.filter(pk=latest_import.pk)
        .prefetch_related("artifacts")
        .first()
    )
    payload = _serialize_import_state(dataset, refreshed)
    payload.update({"message": "Normalization cancelled."})
    return JsonResponse(payload)
