from __future__ import annotations

from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from .models import DatasetArtifact, DatasetImport, sync_import_aggregates


@receiver([post_save, post_delete], sender=DatasetArtifact)
def update_import_aggregates(sender, instance: DatasetArtifact, **kwargs):
    """Recompute import-level byte counters when artifacts change."""

    sync_import_aggregates(instance.dataset_import)


@receiver(post_save, sender=DatasetImport)
def set_total_artifacts_on_create(sender, instance: DatasetImport, created: bool, **kwargs):
    """Ensure artifact counts initialize the first time an import is saved."""

    if created and instance.total_artifacts == 0:
        total = instance.artifacts.count()
        if total:
            DatasetImport.objects.filter(pk=instance.pk).update(
                total_artifacts=total
            )
