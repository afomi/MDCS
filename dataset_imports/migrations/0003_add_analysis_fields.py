from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dataset_imports", "0002_conversation_and_message"),
    ]

    operations = [
        # Add new status choices to DatasetImport
        migrations.AlterField(
            model_name="datasetimport",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("queued", "Queued"),
                    ("downloading", "Downloading"),
                    ("downloaded", "Downloaded"),  # New status
                    ("processing", "Processing"),
                    ("analyzing", "Analyzing"),  # New status
                    ("completed", "Completed"),
                    ("failed", "Failed"),
                    ("cancelled", "Cancelled"),
                ],
                default="pending",
                max_length=32,
                db_index=True,
            ),
        ),
        # Add analysis tracking fields
        migrations.AddField(
            model_name="datasetimport",
            name="analysis_status",
            field=models.CharField(
                blank=True,
                help_text="Separate status for analysis phase (running, completed, failed)",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="datasetimport",
            name="analysis_progress",
            field=models.PositiveIntegerField(
                default=0,
                help_text="Analysis progress percentage (0-100)",
            ),
        ),
        migrations.AddField(
            model_name="datasetimport",
            name="analysis_message",
            field=models.CharField(
                blank=True,
                help_text="Current analysis step description",
                max_length=255,
            ),
        ),
        migrations.AddField(
            model_name="datasetimport",
            name="analysis_started_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="datasetimport",
            name="analysis_finished_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]