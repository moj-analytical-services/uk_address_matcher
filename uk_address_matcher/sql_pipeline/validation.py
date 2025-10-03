from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, Union

import duckdb


@dataclass(frozen=True)
class ColumnSpec:
    """Column name and (normalised) dtype. dtype=None means 'presence only'."""

    name: str
    dtype: Optional[str] = None


def validate_table(
    relation: duckdb.DuckDBPyRelation,
    required: Sequence[Union[str, ColumnSpec]],
    *,
    label: str = "input_table",
    raise_on_error: bool = True,
) -> list[str]:
    """Validate one relation. Returns a list of errors (empty if OK)."""
    normalise = _make_normaliser()
    req_names, req_typed = _coerce_required(required, normalise)
    present_names, typed_pairs, norm_map = _extract_schema(relation, normalise)
    errors = _validate_core(
        label, present_names, typed_pairs, norm_map, req_names, req_typed
    )
    if raise_on_error and errors:
        raise ValueError(_format_errors({label: errors}))
    return errors


def validate_tables(
    relations: Mapping[str, duckdb.DuckDBPyRelation],
    required: Sequence[Union[str, ColumnSpec]],
    *,
    raise_on_error: bool = True,
) -> dict[str, list[str]]:
    """
    Validate many relations at once using the same required spec and aliases.
    Returns {label: [errors...]}, only including failing tables.
    """
    normalise = _make_normaliser()
    req_names, req_typed = _coerce_required(required, normalise)

    all_errors: dict[str, list[str]] = {}
    for label, relation in relations.items():
        present_names, typed_pairs, norm_map = _extract_schema(relation, normalise)
        errs = _validate_core(
            label, present_names, typed_pairs, norm_map, req_names, req_typed
        )
        if errs:
            all_errors[label] = errs

    if raise_on_error and all_errors:
        raise ValueError(_format_errors(all_errors))
    return all_errors


_DEFAULT_ALIASES: dict[str, str] = {
    "TEXT": "VARCHAR",
    "STRING": "VARCHAR",
    "DOUBLE PRECISION": "DOUBLE",
    "FLOAT": "DOUBLE",
    "INT": "INTEGER",
    "BOOL": "BOOLEAN",
}


def _make_normaliser():
    aliases = {**_DEFAULT_ALIASES}

    # prefer longer phrases first (e.g. "DOUBLE PRECISION" before "DOUBLE")
    keys = sorted(aliases.keys(), key=len, reverse=True)
    pattern = (
        re.compile(r"\b(" + "|".join(map(re.escape, keys)) + r")\b", flags=re.I)
        if keys
        else None
    )

    def normalise(s: Optional[str]) -> Optional[str]:
        if s is None:
            return None
        t = str(s).upper().strip()
        t = re.sub(r"\s+", " ", t)
        if pattern:
            t = pattern.sub(lambda m: aliases[m.group(1).upper()], t)
        if "(" in t:
            t = t.split("(", 1)[0].strip()
        return t

    return normalise


def _coerce_required(
    required: Sequence[Union[str, ColumnSpec]],
    normalise,
) -> tuple[set[str], set[ColumnSpec]]:
    names: set[str] = set()
    typed: set[ColumnSpec] = set()
    for item in required:
        if isinstance(item, ColumnSpec):
            names.add(item.name)
            if item.dtype is not None:
                typed.add(ColumnSpec(item.name, normalise(item.dtype)))
        else:
            names.add(str(item))
    return names, typed


def _extract_schema(
    relation: duckdb.DuckDBPyRelation,
    normalise,
) -> tuple[set[str], set[ColumnSpec], dict[str, Optional[str]]]:
    # DuckDB exposes .columns and .dtypes; both are lists
    cols = list(relation.columns)
    dtypes = list(relation.dtypes)  # entries may be duckdb objects; str() handles them

    # normalised dtype map per column
    norm_map: dict[str, Optional[str]] = {n: normalise(t) for n, t in zip(cols, dtypes)}

    present_names = set(cols)
    typed_pairs = {ColumnSpec(n, dt) for n, dt in norm_map.items()}
    return present_names, typed_pairs, norm_map


def _validate_core(
    label: str,
    present_names: set[str],
    typed_pairs: set[ColumnSpec],
    norm_map: dict[str, Optional[str]],
    req_names: set[str],
    req_typed: set[ColumnSpec],
) -> list[str]:
    errors: list[str] = []

    missing = sorted(req_names - present_names)
    errors.extend(f"[{label}] missing column '{m}'" for m in missing)

    for exp in sorted(req_typed, key=lambda c: c.name):
        if exp.name in missing:
            continue  # already reported
        if exp not in typed_pairs:
            actual = norm_map.get(exp.name)
            msg = f"[{label}] column '{exp.name}': expected {exp.dtype}, found {actual if actual is not None else 'unknown'}"
            errors.append(msg)

    return errors


def _format_errors(grouped: Mapping[str, list[str]]) -> str:
    lines = ["Input validation failed:"]
    for label in sorted(grouped.keys()):
        for msg in grouped[label]:
            lines.append(f"  - {msg}")
    return "\n".join(lines)
