from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("dataset_imports", "0006_analysisresult"),
    ]

    operations = [
        migrations.CreateModel(
            name="AnalysisJob",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("requested_mode", models.CharField(blank=True, max_length=64)),
                ("requested_cluster_count", models.PositiveIntegerField(blank=True, null=True)),
                ("algorithm", models.CharField(blank=True, max_length=64)),
                ("status", models.CharField(choices=[("running", "Running"), ("completed", "Completed"), ("failed", "Failed"), ("cancelled", "Cancelled")], default="running", max_length=32)),
                ("task_id", models.CharField(blank=True, max_length=128)),
                ("error", models.TextField(blank=True)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("cluster_count", models.PositiveIntegerField(blank=True, null=True)),
                ("document_count", models.PositiveIntegerField(blank=True, null=True)),
                ("started_at", models.DateTimeField(auto_now_add=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("dataset_import", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="analysis_jobs", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("-started_at",),
            },
        ),
        migrations.AddIndex(
            model_name="analysisjob",
            index=models.Index(fields=["dataset_import", "status"], name="dataset_imp_analysis_status_idx"),
        ),
        migrations.AddIndex(
            model_name="analysisjob",
            index=models.Index(fields=["dataset_import", "started_at"], name="dataset_imp_analysis_started_idx"),
        ),
    ]
