"""Dataset catalog definitions loaded from JSON sources."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class ArtifactDefinition:
    filename: str
    download_url: str = ""
    notes: str = ""
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class DatasetDefinition:
    id: str
    name: str
    description: str
    adapter: Optional[str]
    artifacts: List[ArtifactDefinition]
    notes: str = ""
    supported: bool = True
    license: str = ""
    source_url: str = ""
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class LLMModelDefinition:
    id: str
    provider: str
    name: str
    deployment_options: List[str]
    license: str = ""
    notes: str = ""


DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_SOURCES_PATH = DATA_DIR / "data_sources.json"
LLM_MODELS_PATH = DATA_DIR / "llm_models.json"


def _load_json(path: Path) -> Any:
    if not path.exists():
        raise RuntimeError(f"Expected catalog file at {path!s}, but it was not found")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache(maxsize=1)
def _dataset_map() -> Dict[str, DatasetDefinition]:
    raw = _load_json(DATA_SOURCES_PATH)
    datasets: Dict[str, DatasetDefinition] = {}
    for entry in raw:
        artifacts: List[ArtifactDefinition] = []
        for artifact in entry.get("artifacts", []):
            artifacts.append(
                ArtifactDefinition(
                    filename=artifact["filename"],
                    download_url=artifact.get("download_url", ""),
                    notes=artifact.get("notes", ""),
                    metadata=artifact.get("metadata"),
                )
            )
        adapter_key = entry.get("adapter")
        supported = entry.get("supported")
        if supported is None:
            supported = bool(adapter_key)

        dataset = DatasetDefinition(
            id=entry["id"],
            name=entry.get("name", entry["id"]),
            description=entry.get("description", ""),
            adapter=adapter_key,
            artifacts=artifacts,
            notes=entry.get("notes", ""),
            supported=supported,
            license=entry.get("license", ""),
            source_url=entry.get("source_url", ""),
            tags=entry.get("tags"),
            metadata=entry.get("metadata"),
        )
        datasets[dataset.id] = dataset
    return datasets


@lru_cache(maxsize=1)
def _llm_models() -> List[LLMModelDefinition]:
    if not LLM_MODELS_PATH.exists():
        return []
    raw = _load_json(LLM_MODELS_PATH)
    models: List[LLMModelDefinition] = []
    for entry in raw:
        models.append(
            LLMModelDefinition(
                id=entry["id"],
                provider=entry.get("provider", ""),
                name=entry.get("name", entry["id"]),
                deployment_options=list(entry.get("deployment_options", [])),
                license=entry.get("license", ""),
                notes=entry.get("notes", ""),
            )
        )
    return models


def list_datasets() -> Iterable[DatasetDefinition]:
    """Return all dataset definitions."""

    return list(_dataset_map().values())


def get_dataset(dataset_id: str) -> DatasetDefinition:
    """Return a single dataset definition by identifier."""

    try:
        return _dataset_map()[dataset_id]
    except KeyError as exc:  # pragma: no cover - defensive
        raise KeyError(f"Unknown dataset id '{dataset_id}'") from exc


def list_llm_models() -> Iterable[LLMModelDefinition]:
    """Return configured LLM model definitions."""

    return list(_llm_models())


__all__ = [
    "ArtifactDefinition",
    "DatasetDefinition",
    "LLMModelDefinition",
    "get_dataset",
    "list_datasets",
    "list_llm_models",
]
