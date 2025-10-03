from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dataset_imports", "0005_datasetimportevent"),
    ]

    operations = [
        migrations.CreateModel(
            name="AnalysisResult",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("dataset_id", models.CharField(db_index=True, max_length=128)),
                ("algorithm", models.CharField(help_text="Identifier for the analysis algorithm used.", max_length=64)),
                ("summary", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("dataset_import", models.OneToOneField(on_delete=models.deletion.CASCADE, related_name="analysis_result", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("-created_at",),
            },
        ),
        migrations.AddIndex(
            model_name="analysisresult",
            index=models.Index(fields=["dataset_id"], name="dataset_imports_analysis_dataset_idx"),
        ),
        migrations.AddIndex(
            model_name="analysisresult",
            index=models.Index(fields=["algorithm"], name="dataset_imports_analysis_algorithm_idx"),
        ),
    ]
