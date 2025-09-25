from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable

from django.core.files.storage import Storage

from ..models import DatasetArtifact
from ..normalization import NormalizedConversation


class DatasetAdapter(ABC):
    """Base adapter responsible for turning raw artifacts into normalized conversations."""

    slug: str

    def __init__(self, storage: Storage):
        self.storage = storage

    @abstractmethod
    def process(self, artifact: DatasetArtifact, path: str) -> Iterable[NormalizedConversation]:
        """Yield normalized conversations from a stored artifact path."""

    def open_artifact(self, path: str, mode: str = "rb"):
        return self.storage.open(path, mode)

    def derive_asset_name(self, artifact: DatasetArtifact, suffix: str) -> str:
        source_name = artifact.file.name if artifact.file else artifact.filename
        base = Path(source_name).stem
        return f"{base}{suffix}"


