# Generated manually to bootstrap dataset import metadata tables.
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import dataset_imports.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="DatasetImport",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("slug", models.SlugField(unique=True)),
                ("display_name", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True)),
                ("source_url", models.URLField(blank=True)),
                ("license", models.CharField(blank=True, max_length=255)),
                ("adapter", models.CharField(help_text="Identifier for the adapter module used to normalize records.", max_length=128)),
                ("adapter_version", models.CharField(blank=True, max_length=32)),
                ("status", models.CharField(choices=[
                    ("pending", "Pending"),
                    ("queued", "Queued"),
                    ("downloading", "Downloading"),
                    ("processing", "Processing"),
                    ("completed", "Completed"),
                    ("failed", "Failed"),
                    ("cancelled", "Cancelled"),
                ], db_index=True, default="pending", max_length=32)),
                ("total_bytes", models.BigIntegerField(default=0)),
                ("downloaded_bytes", models.BigIntegerField(default=0)),
                ("processed_bytes", models.BigIntegerField(default=0)),
                ("total_artifacts", models.PositiveIntegerField(default=0)),
                ("processed_artifacts", models.PositiveIntegerField(default=0)),
                ("notes", models.JSONField(blank=True, default=dict)),
                ("last_error", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="DatasetArtifact",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("sequence", models.PositiveIntegerField(default=0, help_text="Relative ordering for multi-part artifacts.")),
                ("filename", models.CharField(max_length=512)),
                ("download_url", models.URLField()),
                ("status", models.CharField(choices=[
                    ("pending", "Pending"),
                    ("downloading", "Downloading"),
                    ("downloaded", "Downloaded"),
                    ("processing", "Processing"),
                    ("completed", "Completed"),
                    ("failed", "Failed"),
                ], db_index=True, default="pending", max_length=32)),
                ("checksum", models.CharField(blank=True, help_text="Checksum (e.g., SHA256) recorded after download.", max_length=128)),
                ("content_type", models.CharField(blank=True, max_length=128)),
                ("file", models.FileField(blank=True, upload_to=dataset_imports.models.artifact_upload_path)),
                ("size_bytes", models.BigIntegerField(default=0)),
                ("downloaded_bytes", models.BigIntegerField(default=0)),
                ("processed_bytes", models.BigIntegerField(default=0)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.TextField(blank=True)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("dataset_import", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="artifacts", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("dataset_import", "sequence", "id"),
            },
        ),
        migrations.CreateModel(
            name="DatasetAsset",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("variant", models.CharField(choices=[
                    ("raw", "Raw copy"),
                    ("staging-jsonl", "Staging JSONL"),
                    ("normalized-jsonl", "Normalized JSONL"),
                    ("normalization-report", "Normalization report"),
                    ("derived", "Derived artifact"),
                ], max_length=64)),
                ("description", models.CharField(blank=True, max_length=255)),
                ("file", models.FileField(max_length=512, upload_to=dataset_imports.models.asset_upload_path)),
                ("size_bytes", models.BigIntegerField(default=0)),
                ("checksum", models.CharField(blank=True, max_length=128)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("artifact", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="assets", to="dataset_imports.datasetartifact")),
                ("dataset_import", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="assets", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="DatasetImportLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("level", models.CharField(choices=[
                    ("DEBUG", "Debug"),
                    ("INFO", "Info"),
                    ("WARNING", "Warning"),
                    ("ERROR", "Error"),
                ], default="INFO", max_length=16)),
                ("message", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("extra", models.JSONField(blank=True, default=dict)),
                ("artifact", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="logs", to="dataset_imports.datasetartifact")),
                ("dataset_import", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="logs", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="DatasetProgressSnapshot",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(max_length=32)),
                ("downloaded_bytes", models.BigIntegerField(default=0)),
                ("processed_bytes", models.BigIntegerField(default=0)),
                ("total_bytes", models.BigIntegerField(default=0)),
                ("message", models.CharField(blank=True, max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("artifact", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="progress_snapshots", to="dataset_imports.datasetartifact")),
                ("dataset_import", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="progress_snapshots", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("-created_at",),
            },
        ),
        migrations.AddField(
            model_name="datasetimport",
            name="created_by",
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="dataset_imports", to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddConstraint(
            model_name="datasetartifact",
            constraint=models.UniqueConstraint(fields=("dataset_import", "filename"), name="dataset_imports_datasetartifact_unique_filename"),
        ),
        migrations.AddConstraint(
            model_name="datasetartifact",
            constraint=models.UniqueConstraint(fields=("dataset_import", "sequence"), name="dataset_imports_datasetartifact_unique_sequence"),
        ),
        migrations.AddIndex(
            model_name="datasetasset",
            index=models.Index(fields=["variant"], name="dataset_ass_variant_050a04_idx"),
        ),
        migrations.AddIndex(
            model_name="datasetasset",
            index=models.Index(fields=["dataset_import", "variant"], name="dataset_ass_dataset_dc3385_idx"),
        ),
        migrations.AddIndex(
            model_name="datasetprogresssnapshot",
            index=models.Index(fields=["dataset_import", "created_at"], name="dataset_pro_dataset_01d923_idx"),
        ),
        migrations.AddIndex(
            model_name="datasetimportlog",
            index=models.Index(fields=["dataset_import", "created_at"], name="dataset_imp_dataset_62050a_idx"),
        ),
        migrations.AddIndex(
            model_name="datasetimportlog",
            index=models.Index(fields=["level"], name="dataset_imp_level_70df0c_idx"),
        ),
    ]
