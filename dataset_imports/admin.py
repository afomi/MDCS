from __future__ import annotations

from django.contrib import admin

from .models import (
    DatasetArtifact,
    DatasetAsset,
    DatasetImport,
    DatasetImportLog,
    DatasetProgressSnapshot,
    ConversationRecord,
    MessageRecord,
)


class DatasetArtifactInline(admin.TabularInline):
    model = DatasetArtifact
    extra = 0
    fields = (
        "sequence",
        "filename",
        "status",
        "size_bytes",
        "downloaded_bytes",
        "processed_bytes",
        "file",
    )
    readonly_fields = ("downloaded_bytes", "processed_bytes")


@admin.register(DatasetImport)
class DatasetImportAdmin(admin.ModelAdmin):
    list_display = (
        "display_name",
        "slug",
        "status",
        "total_artifacts",
        "processed_artifacts",
        "total_bytes",
        "downloaded_bytes",
        "created_at",
    )
    list_filter = ("status", "created_at")
    search_fields = ("display_name", "slug", "description")
    readonly_fields = (
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
        "downloaded_bytes",
        "processed_bytes",
        "total_artifacts",
        "processed_artifacts",
    )
    inlines = [DatasetArtifactInline]


@admin.register(DatasetArtifact)
class DatasetArtifactAdmin(admin.ModelAdmin):
    list_display = (
        "dataset_import",
        "sequence",
        "filename",
        "status",
        "size_bytes",
        "downloaded_bytes",
        "processed_bytes",
    )
    list_filter = ("status", "dataset_import")
    search_fields = ("filename", "download_url")
    readonly_fields = (
        "downloaded_bytes",
        "processed_bytes",
        "started_at",
        "finished_at",
    )


@admin.register(DatasetAsset)
class DatasetAssetAdmin(admin.ModelAdmin):
    list_display = (
        "dataset_import",
        "artifact",
        "variant",
        "size_bytes",
        "created_at",
    )
    list_filter = ("variant", "created_at")
    search_fields = ("description", "file")


@admin.register(DatasetProgressSnapshot)
class DatasetProgressSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        "dataset_import",
        "artifact",
        "status",
        "downloaded_bytes",
        "processed_bytes",
        "total_bytes",
        "created_at",
    )
    list_filter = ("status", "created_at")
    search_fields = ("message",)


@admin.register(DatasetImportLog)
class DatasetImportLogAdmin(admin.ModelAdmin):
    list_display = (
        "dataset_import",
        "artifact",
        "level",
        "created_at",
    )
    list_filter = ("level", "created_at")
    search_fields = ("message",)




@admin.register(ConversationRecord)
class ConversationRecordAdmin(admin.ModelAdmin):
    list_display = (
        "dataset_import",
        "source_id",
        "title",
        "created_at",
    )
    search_fields = ("source_id", "title")
    list_filter = ("dataset_import",)


@admin.register(MessageRecord)
class MessageRecordAdmin(admin.ModelAdmin):
    list_display = (
        "conversation",
        "turn_index",
        "role",
        "lang",
        "created_at",
    )
    list_filter = ("role", "lang")
    search_fields = ("content",)
