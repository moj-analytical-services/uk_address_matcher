from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Callable

import duckdb

from uk_address_matcher.cleaning.pipelines import (
    clean_data_using_precomputed_rel_tok_freq,
    clean_data_with_minimal_steps,
)

if TYPE_CHECKING:
    from benchmarking.datasets.registry import DatasetInfo

# Type aliases for dataset cleaning and loader functions
CleaningFunction = Callable[
    [duckdb.DuckDBPyRelation, duckdb.DuckDBPyConnection], duckdb.DuckDBPyRelation
]
DatasetLoader = Callable[
    [duckdb.DuckDBPyConnection, CleaningFunction], duckdb.DuckDBPyRelation
]


@dataclass(frozen=True)
class DatasetInfo:
    """Information about a benchmark dataset."""

    name: str
    description: str
    source: str
    notes: str = ""

    def summary(self, record_count: int | None = None) -> str:
        """Return a human-readable summary."""
        lines = [
            f"Dataset: {self.name}",
            f"Description: {self.description}",
            f"Source: {self.source}",
        ]
        if record_count is not None:
            lines.append(f"Records: {record_count:,}")
        if self.notes:
            lines.append(f"Notes: {self.notes}")
        return "\n".join(lines)


@dataclass(frozen=True)
class RegisteredDataset:
    """A registered benchmark dataset with its loader function and metadata."""

    name: str
    info: DatasetInfo
    loader: DatasetLoader


_DATASET_REGISTRY: dict[str, RegisteredDataset] = {}


def register_dataset(name: str, info: DatasetInfo, loader: DatasetLoader) -> None:
    if name in _DATASET_REGISTRY:
        raise ValueError(f"Dataset '{name}' is already registered")
    _DATASET_REGISTRY[name] = RegisteredDataset(name=name, info=info, loader=loader)


def get_dataset_info(name: str) -> DatasetInfo:
    if name not in _DATASET_REGISTRY:
        available = ", ".join(_DATASET_REGISTRY.keys())
        raise ValueError(
            f"Unknown dataset: {name}. Available datasets: {available or 'none'}"
        )
    return _DATASET_REGISTRY[name].info


@lru_cache(maxsize=None)
def load_dataset(
    name: str, con: duckdb.DuckDBPyConnection, *, include_term_frequencies: bool
) -> duckdb.DuckDBPyRelation:
    if name not in _DATASET_REGISTRY:
        available = ", ".join(_DATASET_REGISTRY.keys())
        raise ValueError(
            f"Unknown dataset: {name}. Available datasets: {available or 'none'}"
        )
    if include_term_frequencies:
        cleaning_function = clean_data_using_precomputed_rel_tok_freq
    else:
        cleaning_function = clean_data_with_minimal_steps

    registered = _DATASET_REGISTRY[name]
    print(f"Loading {registered.info.name}...")
    return registered.loader(con, cleaning_function)


def list_datasets() -> list[str]:
    return list(_DATASET_REGISTRY.keys())


def get_all_dataset_info() -> dict[str, DatasetInfo]:
    return {name: reg.info for name, reg in _DATASET_REGISTRY.items()}
