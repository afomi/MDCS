from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

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

from .catalog import ArtifactDefinition, DatasetDefinition, get_dataset, list_datasets
from .models import DatasetArtifact, DatasetImport, sync_import_aggregates
from .tasks import run_dataset_import, run_dataset_analysis

DEFAULT_STATUS = {
    "label": "Remote (not downloaded)",
    "badge_class": "bg-secondary",
    "show_check": False,
}

ANALYSIS_MODES: Tuple[Tuple[str, str], ...] = (
    ("top-down", "Top-down (seeded topics)"),
    ("bottom-up", "Bottom-up (clustering-first)"),
    ("hybrid", "Hybrid (combine both)"),
)
DEFAULT_ANALYSIS_MODE = ANALYSIS_MODES[0][0]

STATUS_METADATA = {
    DatasetImport.Status.PENDING: {
        "label": "Queued",
        "badge_class": "bg-secondary",
        "show_check": False,
    },
    DatasetImport.Status.QUEUED: {
        "label": "Queued",
        "badge_class": "bg-info text-dark",
        "show_check": False,
    },
    DatasetImport.Status.DOWNLOADING: {
        "label": "Downloading",
        "badge_class": "bg-info text-dark",
        "show_check": False,
    },
    DatasetImport.Status.PROCESSING: {
        "label": "Processing",
        "badge_class": "bg-warning text-dark",
        "show_check": False,
    },
    DatasetImport.Status.COMPLETED: {
        "label": "Completed",
        "badge_class": "bg-success",
        "show_check": True,
    },
    DatasetImport.Status.DOWNLOADED: {
        "label": "Downloaded",
        "badge_class": "bg-success",
        "show_check": True,
    },
    DatasetImport.Status.ANALYZING: {
        "label": "Analyzing",
        "badge_class": "bg-info text-dark",
        "show_check": False,
    },
    DatasetImport.Status.FAILED: {
        "label": "Failed",
        "badge_class": "bg-danger",
        "show_check": False,
    },
    DatasetImport.Status.CANCELLED: {
        "label": "Cancelled",
        "badge_class": "bg-secondary",
        "show_check": False,
    },
    DatasetArtifact.Status.PENDING: {
        "label": "Pending",
        "badge_class": "bg-secondary",
        "show_check": False,
    },
    DatasetArtifact.Status.DOWNLOADING: {
        "label": "Downloading",
        "badge_class": "bg-info text-dark",
        "show_check": False,
    },
    DatasetArtifact.Status.DOWNLOADED: {
        "label": "Downloaded",
        "badge_class": "bg-success",
        "show_check": True,
    },
    DatasetArtifact.Status.PROCESSING: {
        "label": "Processing",
        "badge_class": "bg-warning text-dark",
        "show_check": False,
    },
    DatasetArtifact.Status.COMPLETED: {
        "label": "Completed",
        "badge_class": "bg-success",
        "show_check": True,
    },
    DatasetArtifact.Status.FAILED: {
        "label": "Failed",
        "badge_class": "bg-danger",
        "show_check": False,
    },
    "not-requested": {
        "label": "Not requested",
        "badge_class": "bg-light text-muted",
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
    }
    return render(request, "dataset_imports/explore.html", context)


def dataset_detail(request: HttpRequest, dataset_id: str) -> HttpResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError as exc:  # pragma: no cover - defensive
        raise Http404(str(exc))

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )

    artifact_rows = _build_artifact_rows(dataset, latest_import)
    can_launch = dataset.supported and (
        latest_import is None
        or latest_import.status
        in {
            DatasetImport.Status.COMPLETED,
            DatasetImport.Status.FAILED,
            DatasetImport.Status.CANCELLED,
        }
    )

    # Analysis controls
    can_analyze = (
        dataset.supported
        and latest_import
        and latest_import.status == DatasetImport.Status.DOWNLOADED
        and latest_import.analysis_status != "running"
    )
    is_analyzing = (
        latest_import
        and latest_import.status == DatasetImport.Status.ANALYZING
    )

    total_artifacts_defined = len(dataset.artifacts)
    latest_analysis_mode = (
        latest_import.notes.get("analysis_mode")
        if latest_import and isinstance(latest_import.notes, dict)
        else None
    )
    latest_artifact_limit = (
        latest_import.notes.get("artifact_limit")
        if latest_import and isinstance(latest_import.notes, dict)
        else None
    )

    downloaded_artifact_count = 0
    if latest_import:
        downloaded_artifact_count = len(
            [
                artifact
                for artifact in latest_import.artifacts.all()
                if artifact.status
                in {
                    DatasetArtifact.Status.DOWNLOADED,
                    DatasetArtifact.Status.COMPLETED,
                }
            ]
        )

    analysis_selection_enabled = bool(latest_import) and downloaded_artifact_count > 0

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

            notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
            notes["analysis_mode"] = requested_analysis_mode
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
            messages.warning(request, "An import is already running for this dataset.")
            return redirect("dataset_imports:detail", dataset_id=dataset.id)

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

        import_obj = _create_import_for_dataset(
            request.user.id,
            dataset,
            analysis_mode=requested_analysis_mode,
            artifact_limit=artifact_limit,
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
    if latest_import:
        total_artifacts_display = latest_import.total_artifacts or total_artifacts_defined
        if total_artifacts_display:
            download_progress_pct = int((downloaded_artifact_count / total_artifacts_display) * 100)

    initial_downloaded_bytes = sum(row["downloaded_bytes"] for row in artifact_rows)

    context = {
        "dataset": dataset,
        "latest_import": latest_import,
        "artifact_rows": artifact_rows,
        "status_label": import_status_meta["label"],
        "status_badge_class": import_status_meta["badge_class"],
        "status_show_check": import_status_meta["show_check"],
        "can_launch": can_launch,
        "analysis_modes": ANALYSIS_MODES,
        "selected_analysis_mode": latest_analysis_mode or DEFAULT_ANALYSIS_MODE,
        "total_artifacts_defined": total_artifacts_defined,
        "selected_artifact_limit": latest_artifact_limit or total_artifacts_defined,
        "artifact_limit_options": list(range(1, total_artifacts_defined + 1)) if total_artifacts_defined else [],
        "analysis_mode_labels": analysis_mode_labels,
        "latest_analysis_label": latest_analysis_label,

        # Analysis controls
        "can_analyze": can_analyze,
        "is_analyzing": is_analyzing,
        "analysis_selection_enabled": analysis_selection_enabled,
        "downloaded_artifact_count": downloaded_artifact_count,
        "download_progress_pct": download_progress_pct,
        "total_artifacts_display": total_artifacts_display,
        "initial_downloaded_bytes": initial_downloaded_bytes,
        "status_stream_url": reverse("dataset_imports:status-stream", args=[dataset.id]),
        "analysis_summary": latest_import.notes.get("analysis_summary") if latest_import and isinstance(latest_import.notes, dict) else None,
    }
    return render(request, "dataset_imports/detail.html", context)


def _build_artifact_rows(dataset: DatasetDefinition, latest_import: DatasetImport | None) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    artifact_map = {}
    artifact_limit = None
    if latest_import:
        artifact_map = {
            artifact.filename: artifact for artifact in latest_import.artifacts.all()
        }
        if isinstance(latest_import.notes, dict):
            artifact_limit = latest_import.notes.get("artifact_limit")

    for index, artifact_def in enumerate(dataset.artifacts, start=1):
        matching = artifact_map.get(artifact_def.filename)
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

        rows.append(
            {
                "sequence": index,
                "definition": artifact_def,
                "status": status_value,
                "status_label": status_meta["label"],
                "status_badge_class": status_meta["badge_class"],
                "status_show_check": status_meta["show_check"],
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
            }
        )
    return rows


def _create_import_for_dataset(
    user_id: int | None,
    dataset: DatasetDefinition,
    *,
    analysis_mode: str | None = None,
    artifact_limit: int | None = None,
) -> DatasetImport:
    timestamp = timezone.now().strftime("%Y%m%d%H%M%S")
    slug = slugify(f"{dataset.id}-{timestamp}")

    if not dataset.adapter:
        raise ValueError(f"Dataset {dataset.id} does not declare an adapter")

    normalized_analysis_mode = analysis_mode or DEFAULT_ANALYSIS_MODE
    valid_modes = {mode for mode, _ in ANALYSIS_MODES}
    if normalized_analysis_mode not in valid_modes:
        normalized_analysis_mode = DEFAULT_ANALYSIS_MODE

    total_defined = len(dataset.artifacts)
    selected_limit = artifact_limit if artifact_limit is not None else total_defined
    if total_defined:
        selected_limit = max(1, min(selected_limit, total_defined))
    else:
        selected_limit = 0

    selected_artifacts = [artifact.filename for artifact in dataset.artifacts[:selected_limit]]

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
            "artifact_limit": selected_limit,
            "available_artifacts": [artifact.filename for artifact in dataset.artifacts],
            "selected_artifacts": selected_artifacts,
        },
        created_by_id=user_id,
    )

    artifacts = []
    for sequence, artifact_def in enumerate(dataset.artifacts):
        if selected_limit and sequence >= selected_limit:
            break
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

    if not latest_import:
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
            "processed_bytes": 0,
            "processed_artifacts": 0,
            "total_artifacts": len(artifacts_payload),
            "download_progress": 0,
            "analysis_mode": None,
            "artifact_limit": len(artifacts_payload) or None,
            "selected_artifacts": [],
            "analysis_status": "",
            "analysis_progress": 0,
        "analysis_message": "",
        "analysis_started_at": None,
        "analysis_finished_at": None,
        "can_analyze": False,
        "is_analyzing": False,
        "artifacts": artifacts_payload,
        "downloaded_artifacts": 0,
        "analysis_summary": None,
    }

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
    if total_artifacts:
        download_progress = int((downloaded_artifacts / total_artifacts) * 100)

    downloaded_bytes_total = sum(row["downloaded_bytes"] for row in artifacts_payload)
    processed_bytes_total = sum(row["processed_bytes"] for row in artifacts_payload)

    status_badge_class = import_status_metadata["badge_class"]
    status_show_check = import_status_metadata["show_check"]
    status_label = import_status_metadata["label"]
    if latest_import.status == DatasetImport.Status.QUEUED and downloaded_bytes_total == 0:
        status_label = "Queued"
        status_badge_class = "bg-secondary"
        status_show_check = False

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
        "artifact_limit": notes.get("artifact_limit"),
        "selected_artifacts": notes.get("selected_artifacts", []),
        "artifacts": artifacts_payload,
        "downloaded_artifacts": downloaded_artifacts,
        "analysis_status": latest_import.analysis_status or "",
        "analysis_progress": latest_import.analysis_progress or 0,
        "analysis_message": latest_import.analysis_message or "",
        "analysis_started_at": latest_import.analysis_started_at.isoformat()
        if latest_import.analysis_started_at
        else None,
        "analysis_finished_at": latest_import.analysis_finished_at.isoformat()
        if latest_import.analysis_finished_at
        else None,
        "can_analyze": latest_import.status == DatasetImport.Status.DOWNLOADED,
        "is_analyzing": latest_import.status == DatasetImport.Status.ANALYZING,
        "analysis_summary": notes.get("analysis_summary"),
    }


def status_api(request: HttpRequest, dataset_id: str) -> JsonResponse:
    try:
        dataset = get_dataset(dataset_id)
    except KeyError:
        raise Http404("Unknown dataset")

    latest_import = (
        DatasetImport.objects.filter(notes__dataset_id=dataset.id)
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

    with transaction.atomic():
        import_obj = _create_import_for_dataset(
            request.user.id,
            dataset,
            analysis_mode=requested_analysis_mode,
            artifact_limit=artifact_limit,
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
        .prefetch_related("artifacts")
        .order_by("-created_at")
        .first()
    )

    if not latest_import:
        payload = _serialize_import_state(dataset, None)
        payload.update({"error": "No import found for this dataset"})
        return JsonResponse(payload, status=400)

    payload = _serialize_import_state(dataset, latest_import)
    if latest_import.status != DatasetImport.Status.DOWNLOADED:
        payload.update(
            {
                "error": (
                    "Import must be downloaded before analysis. "
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

    requested_analysis_mode = (data.get("analysis_mode") or "").strip().lower()
    if requested_analysis_mode:
        valid_modes = {mode for mode, _ in ANALYSIS_MODES}
        if requested_analysis_mode not in valid_modes:
            payload.update({"error": "Unknown analysis approach."})
            return JsonResponse(payload, status=400)

        notes = latest_import.notes if isinstance(latest_import.notes, dict) else {}
        notes["analysis_mode"] = requested_analysis_mode
        latest_import.notes = notes
        latest_import.save(update_fields=["notes", "updated_at"])
    elif isinstance(latest_import.notes, dict) and latest_import.notes.get("analysis_mode"):
        requested_analysis_mode = latest_import.notes.get("analysis_mode")
    else:
        requested_analysis_mode = DEFAULT_ANALYSIS_MODE

    # Start analysis
    try:
        latest_import.start_analysis()
        run_dataset_analysis.delay(latest_import.pk)
    except Exception as exc:  # pragma: no cover - celery misconfiguration
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
