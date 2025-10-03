from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("dataset_imports", "0004_rename_dataset_ass_variant_050a04_idx_dataset_imp_variant_e200b9_idx_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="DatasetImportEvent",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("event_type", models.CharField(choices=[
                    ("download_start", "Download started"),
                    ("download_complete", "Download complete"),
                    ("analysis_start", "Analysis started"),
                    ("analysis_complete", "Analysis complete"),
                    ("analysis_error", "Analysis error"),
                    ("analysis_cancel", "Analysis cancelled"),
                ], max_length=32)),
                ("message", models.CharField(blank=True, max_length=255)),
                ("details", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "dataset_import",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="events",
                        to="dataset_imports.datasetimport",
                    ),
                ),
            ],
            options={
                "ordering": ("-created_at", "-id"),
            },
        ),
        migrations.AddIndex(
            model_name="datasetimportevent",
            index=models.Index(fields=["dataset_import", "created_at"], name="dataset_imp_ev_dataset_5217e8_idx"),
        ),
        migrations.AddIndex(
            model_name="datasetimportevent",
            index=models.Index(fields=["event_type"], name="dataset_imp_ev_event_t_baaac5_idx"),
        ),
    ]
