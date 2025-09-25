from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("dataset_imports", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="ConversationRecord",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("source_id", models.CharField(max_length=255)),
                ("title", models.CharField(blank=True, max_length=255)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("dataset_import", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="conversations", to="dataset_imports.datasetimport")),
            ],
            options={
                "ordering": ("dataset_import", "id"),
            },
        ),
        migrations.CreateModel(
            name="MessageRecord",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("turn_index", models.PositiveIntegerField()),
                ("parent_turn_index", models.IntegerField(blank=True, null=True)),
                ("role", models.CharField(max_length=32)),
                ("content", models.TextField()),
                ("lang", models.CharField(blank=True, max_length=8)),
                ("content_hash", models.CharField(blank=True, max_length=64)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("conversation", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="messages", to="dataset_imports.conversationrecord")),
            ],
            options={
                "ordering": ("conversation", "turn_index"),
            },
        ),
        migrations.AddConstraint(
            model_name="conversationrecord",
            constraint=models.UniqueConstraint(fields=("dataset_import", "source_id"), name="dataset_imports_conversation_unique_source"),
        ),
        migrations.AddConstraint(
            model_name="messagerecord",
            constraint=models.UniqueConstraint(fields=("conversation", "turn_index"), name="dataset_imports_message_unique_turn"),
        ),
    ]
