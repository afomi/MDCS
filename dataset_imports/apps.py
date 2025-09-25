from django.apps import AppConfig


class DatasetImportsConfig(AppConfig):
    name = "dataset_imports"
    verbose_name = "Dataset Imports"

    def ready(self) -> None:
        # Import signal handlers so status aggregation stays in sync.
        from . import signals
