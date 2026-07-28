from __future__ import annotations

import json
import logging
import shutil
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from uk_address_matcher import __version__
from uk_address_matcher.labelling.extraction import build_final_review_relation
from uk_address_matcher.labelling.manifest import build_manifest
from uk_address_matcher.labelling.schema import (
    DEFAULT_LABELLING_BUNDLE_DIRECTORY,
    DEFAULT_TOP_N_CANDIDATES,
)
from uk_address_matcher.labelling.validation import (
    validate_export_arguments,
    validate_output_directory,
    validate_source_relations,
    validate_splink_relations,
    validate_written_parquet,
)

if TYPE_CHECKING:
    from uk_address_matcher.post_linkage.match_result.result import MatchResult

_CANONICAL_LABEL_COLUMN = "unique_id"
_MESSY_COLUMNS: tuple[str, ...] = ()
_CANONICAL_COLUMNS: tuple[str, ...] = ()

logger = logging.getLogger("uk_address_matcher")


def export_labelling_bundle(
    match_result: MatchResult,
    output_directory: str | Path = DEFAULT_LABELLING_BUNDLE_DIRECTORY,
    *,
    top_n_candidates: int = DEFAULT_TOP_N_CANDIDATES,
    overwrite: bool = False,
) -> Path:
    """Export a self-contained Parquet bundle for later human review."""
    output_path = validate_export_arguments(
        output_directory=output_directory,
        top_n_candidates=top_n_candidates,
    )
    validate_output_directory(output_path, overwrite=overwrite)
    canonical_id_type, canonical_label_type = validate_source_relations(
        con=match_result.con,
        messy_relation=match_result._messy_relation,
        canonical_relation=match_result._canonical_relation,
        canonical_label_column=_CANONICAL_LABEL_COLUMN,
        messy_columns=_MESSY_COLUMNS,
        canonical_columns=_CANONICAL_COLUMNS,
    )
    validate_splink_relations(match_result)

    bundle_id = str(uuid.uuid4())
    created_at_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    review_relation = build_final_review_relation(
        match_result=match_result,
        bundle_id=bundle_id,
        uk_address_matcher_version=__version__,
        created_at_utc=created_at_utc,
        top_n_candidates=top_n_candidates,
        canonical_label_column=_CANONICAL_LABEL_COLUMN,
        messy_columns=_MESSY_COLUMNS,
        canonical_columns=_CANONICAL_COLUMNS,
        canonical_id_type=canonical_id_type,
        canonical_label_type=canonical_label_type,
    )
    expected_row_count = match_result._messy_relation.count("*").fetchone()[0]

    temporary_path = Path(
        tempfile.mkdtemp(
            prefix=f".{output_path.name}.tmp-",
            dir=output_path.parent,
        )
    )
    try:
        parquet_path = temporary_path / "review_data.parquet"
        _write_parquet(match_result, review_relation, parquet_path)
        parquet_validation = validate_written_parquet(
            parquet_path,
            expected_row_count=int(expected_row_count),
        )
        manifest = build_manifest(
            match_result=match_result,
            bundle_id=bundle_id,
            created_at_utc=created_at_utc,
            uk_address_matcher_version=__version__,
            parquet_validation=parquet_validation,
            top_n_candidates=top_n_candidates,
            canonical_label_column=_CANONICAL_LABEL_COLUMN,
            messy_columns=_MESSY_COLUMNS,
            canonical_columns=_CANONICAL_COLUMNS,
        )
        _write_manifest(temporary_path / "manifest.json", manifest)
        _publish_bundle(temporary_path, output_path, overwrite=overwrite)
        logger.info(
            "Labelling bundle written to '%s' (bundle_id=%s)",
            output_path,
            bundle_id,
        )
    except Exception:
        shutil.rmtree(temporary_path, ignore_errors=True)
        raise
    return output_path


def _write_parquet(
    match_result: MatchResult,
    review_relation: object,
    parquet_path: Path,
) -> None:
    parquet_sql = str(parquet_path).replace("'", "''")
    match_result.con.execute(
        f"""
        COPY ({review_relation.sql_query()}) TO '{parquet_sql}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )


def _write_manifest(path: Path, manifest: dict[str, object]) -> None:
    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)


def _publish_bundle(temporary_path: Path, output_path: Path, *, overwrite: bool) -> None:
    if not output_path.exists():
        temporary_path.rename(output_path)
        return
    if not overwrite:
        raise FileExistsError(f"Output directory already exists: {output_path}")
    backup_path = output_path.with_name(f".{output_path.name}.old-{uuid.uuid4().hex}")
    output_path.rename(backup_path)
    try:
        temporary_path.rename(output_path)
    except Exception:
        backup_path.rename(output_path)
        raise
    shutil.rmtree(backup_path)
