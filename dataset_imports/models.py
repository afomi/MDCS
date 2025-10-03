from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


def artifact_upload_path(instance, filename):
    slug = instance.dataset_import.slug
    return f"dataset_artifacts/{slug}/{filename}"


def asset_upload_path(instance, filename):
    slug = instance.dataset_import.slug
    return f"dataset_assets/{slug}/{filename}"


class DatasetImport(models.Model):
    """Tracks a logical ingestion job for a dataset download and normalization."""

    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        QUEUED = "queued", _("Queued")
        DOWNLOADING = "downloading", _("Downloading")
        DOWNLOADED = "downloaded", _("Downloaded")  # New: Files ready for analysis
        PROCESSING = "processing", _("Processing")  # Legacy: Combined processing
        ANALYZING = "analyzing", _("Analyzing")  # New: Analysis in progress
        COMPLETED = "completed", _("Completed")
        FAILED = "failed", _("Failed")
        CANCELLED = "cancelled", _("Cancelled")

    slug = models.SlugField(unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    source_url = models.URLField(blank=True)
    license = models.CharField(max_length=255, blank=True)
    adapter = models.CharField(
        max_length=128,
        help_text=_("Identifier for the adapter module used to normalize records."),
    )
    adapter_version = models.CharField(max_length=32, blank=True)
    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    total_bytes = models.BigIntegerField(default=0)
    downloaded_bytes = models.BigIntegerField(default=0)
    processed_bytes = models.BigIntegerField(default=0)
    total_artifacts = models.PositiveIntegerField(default=0)
    processed_artifacts = models.PositiveIntegerField(default=0)
    notes = models.JSONField(default=dict, blank=True)
    last_error = models.TextField(blank=True)

    # Analysis tracking fields
    analysis_status = models.CharField(
        max_length=32,
        blank=True,
        null=True,
        help_text=_("Separate status for analysis phase (running, completed, failed)"),
    )
    analysis_progress = models.PositiveIntegerField(
        default=0,
        null=True,
        blank=True,
        help_text=_("Analysis progress percentage (0-100)"),
    )
    analysis_message = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text=_("Current analysis step description"),
    )
    analysis_started_at = models.DateTimeField(null=True, blank=True)
    analysis_finished_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="dataset_imports",
    )

    class Meta:
        ordering = ("-created_at",)

    def __str__(self) -> str:  # pragma: no cover - representation helper
        return f"{self.display_name} ({self.slug})"

    def mark_started(self) -> None:
        if not self.started_at:
            self.started_at = timezone.now()
        self.status = self.Status.DOWNLOADING
        self.save(update_fields=["started_at", "status", "updated_at"])

    def mark_finished(self, *, success: bool) -> None:
        self.finished_at = timezone.now()
        self.status = self.Status.COMPLETED if success else self.Status.FAILED
        fields = ["finished_at", "status", "updated_at"]
        if success:
            self.last_error = ""
            fields.append("last_error")
        self.save(update_fields=fields)

    def mark_downloaded(self) -> None:
        """Mark import as downloaded (files ready for analysis)."""
        self.status = self.Status.DOWNLOADED
        self.save(update_fields=["status", "updated_at"])

    def start_analysis(self) -> None:
        """Start the analysis phase."""
        self.status = self.Status.ANALYZING
        self.analysis_status = "running"
        self.analysis_progress = 0
        self.analysis_message = "Starting analysis..."
        self.analysis_started_at = timezone.now()
        self.save(update_fields=[
            "status", "analysis_status", "analysis_progress",
            "analysis_message", "analysis_started_at", "updated_at"
        ])

    def update_analysis_progress(self, progress: int, message: str = "") -> None:
        """Update analysis progress and message."""
        self.analysis_progress = max(0, min(progress, 100))
        if message:
            self.analysis_message = message[:255]
        self.save(update_fields=["analysis_progress", "analysis_message", "updated_at"])

    def mark_analysis_completed(self, *, success: bool) -> None:
        """Mark analysis as completed or failed."""
        self.analysis_finished_at = timezone.now()
        if success:
            self.analysis_status = "completed"
            self.analysis_progress = 100
            self.analysis_message = "Analysis completed"
            self.status = self.Status.COMPLETED
        else:
            self.analysis_status = "failed"
            self.analysis_message = "Analysis failed"

        self.save(update_fields=[
            "analysis_status", "analysis_progress", "analysis_message",
            "analysis_finished_at", "status", "updated_at"
        ])

    def reset_analysis(self) -> None:
        """Return the import to a downloaded state without removing artifacts."""

        notes = dict(self.notes or {})
        notes.pop("analysis_task_id", None)
        notes.pop("analysis_job_id", None)

        fields = {
            "status": self.Status.DOWNLOADED,
            "analysis_status": "",
            "analysis_progress": 0,
            "analysis_message": "",
            "analysis_started_at": None,
            "analysis_finished_at": None,
            "updated_at": timezone.now(),
            "notes": notes,
        }

        DatasetImport.objects.filter(pk=self.pk).update(**fields)
        for key, value in fields.items():
            setattr(self, key, value)

    def cancel_analysis(self, *, reason: str | None = None) -> None:
        """Cancel in-flight analysis while keeping downloaded artifacts."""

        now = timezone.now()
        message = (reason or "Analysis cancelled by user")[:255]
        notes = dict(self.notes or {})
        notes.pop("analysis_task_id", None)
        notes.pop("analysis_job_id", None)
        fields = {
            "status": self.Status.DOWNLOADED,
            "analysis_status": "cancelled",
            "analysis_progress": 0,
            "analysis_message": message,
            "analysis_finished_at": now,
            "updated_at": now,
            "notes": notes,
        }

        DatasetImport.objects.filter(pk=self.pk).update(**fields)
        for key, value in fields.items():
            setattr(self, key, value)
        self.notes = notes

    def set_analysis_task_id(self, task_id: str | None) -> None:
        """Record or clear the Celery task identifier for analysis."""

        notes = dict(self.notes or {})
        if task_id:
            notes["analysis_task_id"] = task_id
        else:
            notes.pop("analysis_task_id", None)

        now = timezone.now()

        DatasetImport.objects.filter(pk=self.pk).update(
            notes=notes,
            updated_at=now,
        )
        self.notes = notes
        self.updated_at = now


class DatasetArtifact(models.Model):
    """Represents a downloadable file that belongs to an import."""

    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        DOWNLOADING = "downloading", _("Downloading")
        DOWNLOADED = "downloaded", _("Downloaded")
        PROCESSING = "processing", _("Processing")
        COMPLETED = "completed", _("Completed")
        FAILED = "failed", _("Failed")

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="artifacts",
    )
    sequence = models.PositiveIntegerField(
        default=0,
        help_text=_("Relative ordering for multi-part artifacts."),
    )
    filename = models.CharField(max_length=512)
    download_url = models.URLField()
    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    checksum = models.CharField(
        max_length=128,
        blank=True,
        help_text=_("Checksum (e.g., SHA256) recorded after download."),
    )
    content_type = models.CharField(max_length=128, blank=True)
    file = models.FileField(
        upload_to=artifact_upload_path,
        blank=True,
    )
    size_bytes = models.BigIntegerField(default=0)
    downloaded_bytes = models.BigIntegerField(default=0)
    processed_bytes = models.BigIntegerField(default=0)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("dataset_import", "sequence", "id")
        constraints = [
            models.UniqueConstraint(
                fields=("dataset_import", "filename"),
                name="dataset_imports_datasetartifact_unique_filename",
            ),
            models.UniqueConstraint(
                fields=("dataset_import", "sequence"),
                name="dataset_imports_datasetartifact_unique_sequence",
            ),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"Artifact {self.filename} ({self.dataset_import.slug})"


class DatasetAsset(models.Model):
    """Files produced during normalization (staging outputs, logs, derived data)."""

    class Variant(models.TextChoices):
        RAW = "raw", _("Raw copy")
        STAGING_JSONL = "staging-jsonl", _("Staging JSONL")
        NORMALIZED_JSONL = "normalized-jsonl", _("Normalized JSONL")
        NORMALIZATION_REPORT = "normalization-report", _("Normalization report")
        DERIVED = "derived", _("Derived artifact")

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="assets",
    )
    artifact = models.ForeignKey(
        DatasetArtifact,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="assets",
    )
    variant = models.CharField(max_length=64, choices=Variant.choices)
    description = models.CharField(max_length=255, blank=True)
    file = models.FileField(
        upload_to=asset_upload_path,
        max_length=512,
    )
    size_bytes = models.BigIntegerField(default=0)
    checksum = models.CharField(max_length=128, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["variant"]),
            models.Index(fields=["dataset_import", "variant"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"{self.dataset_import.slug}:{self.variant}:{self.id}"


class AnalysisResult(models.Model):
    """Persisted summary produced by the analysis pipeline."""

    dataset_import = models.OneToOneField(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="analysis_result",
    )
    dataset_id = models.CharField(max_length=128, db_index=True)
    algorithm = models.CharField(
        max_length=64,
        help_text=_("Identifier for the analysis algorithm used."),
    )
    summary = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["dataset_id"]),
            models.Index(fields=["algorithm"]),
        ]

    def __str__(self) -> str:  # pragma: no cover - admin helper
        return f"AnalysisResult(import={self.dataset_import_id}, algorithm={self.algorithm})"


class AnalysisJob(models.Model):
    """Lifecycle record for a dataset analysis execution."""

    class Status(models.TextChoices):
        RUNNING = "running", _("Running")
        COMPLETED = "completed", _("Completed")
        FAILED = "failed", _("Failed")
        CANCELLED = "cancelled", _("Cancelled")

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="analysis_jobs",
    )
    requested_mode = models.CharField(max_length=64, blank=True)
    requested_cluster_count = models.PositiveIntegerField(null=True, blank=True)
    algorithm = models.CharField(max_length=64, blank=True)
    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.RUNNING,
    )
    task_id = models.CharField(max_length=128, blank=True)
    error = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    cluster_count = models.PositiveIntegerField(null=True, blank=True)
    document_count = models.PositiveIntegerField(null=True, blank=True)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-started_at",)
        indexes = [
            models.Index(fields=["dataset_import", "status"]),
            models.Index(fields=["dataset_import", "started_at"]),
        ]

    def mark_complete(self, *, algorithm: str | None = None, clusters: int | None = None, documents: int | None = None, metadata: dict | None = None) -> None:
        self.status = self.Status.COMPLETED
        self.finished_at = timezone.now()
        if algorithm:
            self.algorithm = algorithm
        if clusters is not None:
            self.cluster_count = clusters
        if documents is not None:
            self.document_count = documents
        if metadata is not None:
            self.metadata = metadata
        self.error = ""
        self.save(update_fields=[
            "status",
            "finished_at",
            "algorithm",
            "cluster_count",
            "document_count",
            "metadata",
            "error",
            "updated_at",
        ])

    def mark_failed(self, message: str) -> None:
        self.status = self.Status.FAILED
        self.finished_at = timezone.now()
        self.error = message[:2000]
        self.save(update_fields=["status", "finished_at", "error", "updated_at"])

    def mark_cancelled(self, message: str | None = None) -> None:
        self.status = self.Status.CANCELLED
        self.finished_at = timezone.now()
        if message:
            self.error = message[:2000]
        self.save(update_fields=["status", "finished_at", "error", "updated_at"])


class DatasetProgressSnapshot(models.Model):
    """Immutable record of progress used for auditing UI updates."""

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="progress_snapshots",
    )
    artifact = models.ForeignKey(
        DatasetArtifact,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="progress_snapshots",
    )
    status = models.CharField(max_length=32)
    downloaded_bytes = models.BigIntegerField(default=0)
    processed_bytes = models.BigIntegerField(default=0)
    total_bytes = models.BigIntegerField(default=0)
    message = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["dataset_import", "created_at"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"Snapshot {self.dataset_import_id} @ {self.created_at:%Y-%m-%d %H:%M:%S}"


class DatasetImportLog(models.Model):
    """Detailed log messages emitted by tasks during an import."""

    LEVEL_CHOICES = (
        ("DEBUG", "Debug"),
        ("INFO", "Info"),
        ("WARNING", "Warning"),
        ("ERROR", "Error"),
    )

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="logs",
    )
    artifact = models.ForeignKey(
        DatasetArtifact,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="logs",
    )
    level = models.CharField(max_length=16, choices=LEVEL_CHOICES, default="INFO")
    message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    extra = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["dataset_import", "created_at"]),
            models.Index(fields=["level"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"[{self.level}] {self.dataset_import.slug}: {self.message[:60]}"


class DatasetImportEvent(models.Model):
    """Human-friendly audit events for key ingestion milestones."""

    class Type(models.TextChoices):
        DOWNLOAD_START = "download_start", _("Download started")
        DOWNLOAD_COMPLETE = "download_complete", _("Download complete")
        ANALYSIS_START = "analysis_start", _("Analysis started")
        ANALYSIS_COMPLETE = "analysis_complete", _("Analysis complete")
        ANALYSIS_ERROR = "analysis_error", _("Analysis error")
        ANALYSIS_CANCEL = "analysis_cancel", _("Analysis cancelled")

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="events",
    )
    event_type = models.CharField(max_length=32, choices=Type.choices)
    message = models.CharField(max_length=255, blank=True)
    details = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at", "-id")
        indexes = [
            models.Index(fields=["dataset_import", "created_at"]),
            models.Index(fields=["event_type"]),
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"{self.dataset_import.slug}:{self.event_type}:{self.created_at:%Y-%m-%d %H:%M:%S}"


def log_dataset_event(
    import_obj: DatasetImport,
    event_type: str,
    *,
    message: str = "",
    details: dict | None = None,
) -> DatasetImportEvent:
    return DatasetImportEvent.objects.create(
        dataset_import=import_obj,
        event_type=event_type,
        message=(message or "")[:255],
        details=details or {},
    )


class ConversationRecord(models.Model):
    """Normalized conversation persisted for downstream use."""

    dataset_import = models.ForeignKey(
        DatasetImport,
        on_delete=models.CASCADE,
        related_name="conversations",
    )
    source_id = models.CharField(max_length=255)
    title = models.CharField(max_length=255, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("dataset_import", "id")
        constraints = [
            models.UniqueConstraint(
                fields=("dataset_import", "source_id"),
                name="dataset_imports_conversation_unique_source",
            )
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"{self.dataset_import.slug}:{self.source_id}"


class MessageRecord(models.Model):
    """Normalized message belonging to a conversation."""

    conversation = models.ForeignKey(
        ConversationRecord,
        on_delete=models.CASCADE,
        related_name="messages",
    )
    turn_index = models.PositiveIntegerField()
    parent_turn_index = models.IntegerField(null=True, blank=True)
    role = models.CharField(max_length=32)
    content = models.TextField()
    lang = models.CharField(max_length=8, blank=True)
    content_hash = models.CharField(max_length=64, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("conversation", "turn_index")
        constraints = [
            models.UniqueConstraint(
                fields=("conversation", "turn_index"),
                name="dataset_imports_message_unique_turn",
            )
        ]

    def __str__(self) -> str:  # pragma: no cover
        return f"{self.conversation_id}:{self.turn_index}:{self.role}"


def sync_import_aggregates(import_obj: DatasetImport) -> None:
    """Recalculate aggregate counters from artifact rows."""

    aggregates = import_obj.artifacts.aggregate(
        total_bytes=models.Sum("size_bytes"),
        downloaded=models.Sum("downloaded_bytes"),
        processed=models.Sum("processed_bytes"),
        total=models.Count("id"),
        completed=models.Count(
            "id",
            filter=models.Q(status=DatasetArtifact.Status.COMPLETED),
        ),
    )

    import_obj.total_bytes = aggregates.get("total_bytes") or 0
    import_obj.downloaded_bytes = aggregates.get("downloaded") or 0
    import_obj.processed_bytes = aggregates.get("processed") or 0
    import_obj.total_artifacts = aggregates.get("total") or 0
    import_obj.processed_artifacts = aggregates.get("completed") or 0
    import_obj.save(
        update_fields=[
            "total_bytes",
            "downloaded_bytes",
            "processed_bytes",
            "total_artifacts",
            "processed_artifacts",
            "updated_at",
        ]
    )

    reconcile_import_status(import_obj)


def reconcile_import_status(
    import_obj: DatasetImport,
    *,
    download_complete: bool | None = None,
    has_failures: bool | None = None,
) -> None:
    """Ensure the import status reflects the current artifact download state."""

    allowed_statuses = {
        DatasetImport.Status.PENDING,
        DatasetImport.Status.QUEUED,
        DatasetImport.Status.DOWNLOADING,
    }

    if import_obj.status not in allowed_statuses:
        return

    if download_complete is None:
        completed_download_states = (
            DatasetArtifact.Status.DOWNLOADED,
            DatasetArtifact.Status.COMPLETED,
            DatasetArtifact.Status.FAILED,
        )

        pending_downloads = import_obj.artifacts.exclude(
            status__in=completed_download_states
        ).exists()
        if pending_downloads:
            return
    elif not download_complete:
        return

    if has_failures is None:
        failed_downloads = import_obj.artifacts.filter(
            status=DatasetArtifact.Status.FAILED
        ).exists()
    else:
        failed_downloads = has_failures
    now = timezone.now()

    if failed_downloads:
        update_kwargs = {
            "status": DatasetImport.Status.FAILED,
            "updated_at": now,
        }
        if not import_obj.finished_at:
            update_kwargs["finished_at"] = now
        updated = DatasetImport.objects.filter(pk=import_obj.pk).update(**update_kwargs)
        if updated:
            import_obj.status = DatasetImport.Status.FAILED
            import_obj.updated_at = now
            if "finished_at" in update_kwargs:
                import_obj.finished_at = update_kwargs["finished_at"]
        return

    updated = (
        DatasetImport.objects.filter(pk=import_obj.pk)
        .exclude(status=DatasetImport.Status.DOWNLOADED)
        .update(status=DatasetImport.Status.DOWNLOADED, updated_at=now)
    )
    if updated:
        import_obj.status = DatasetImport.Status.DOWNLOADED
        import_obj.updated_at = now
