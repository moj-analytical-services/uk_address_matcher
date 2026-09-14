from __future__ import annotations

import json
import logging
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
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
    validate_record_selection_arguments,
    validate_source_relations,
    validate_splink_relations,
    validate_written_parquet,
)

if TYPE_CHECKING:
    from uk_address_matcher.post_linkage.match_result.result import MatchResult

_CANONICAL_LABEL_COLUMN = "ukam_address_id"
_CANONICAL_DATA_FILE = "canonical_data.parquet"
_MESSY_COLUMNS: tuple[str, ...] = ()
_CANONICAL_COLUMNS: tuple[str, ...] = ()

logger = logging.getLogger("uk_address_matcher")


def _export_labelling_bundle_beta(
    match_result: MatchResult,
    output_directory: str | Path = DEFAULT_LABELLING_BUNDLE_DIRECTORY,
    *,
    top_n_candidates: int = DEFAULT_TOP_N_CANDIDATES,
    overwrite: bool = False,
    total_records_to_export: int | None = None,
    review_data_chunk_count: int = 1,
) -> Path:
    """Export a self-contained Parquet bundle for later human review.

    Args:
        match_result: Completed matching result containing the records to review.
        output_directory: Directory in which to create the bundle.
        top_n_candidates: Maximum number of candidates to include for each record.
        overwrite: Replace an existing labelling bundle in ``output_directory``.
        total_records_to_export: Exact number of messy records to export. ``None``
            exports every retained messy record. Records are selected in ascending
            ``unique_id`` order so the export is reproducible.
        review_data_chunk_count: Number of review-data Parquet files to create.
            Records are divided into contiguous chunks whose sizes differ by at most
            one record. A value of ``1`` keeps the single-file bundle format.

    Returns:
        The resolved path to the created bundle directory.

    Raises:
        TypeError: If an integer argument has the wrong type.
        ValueError: If the requested record count or chunk count is invalid.
        FileExistsError: If the output directory is already populated and
            ``overwrite`` is false.
    """
    output_path = validate_export_arguments(
        output_directory=output_directory,
        top_n_candidates=top_n_candidates,
        total_records_to_export=total_records_to_export,
        review_data_chunk_count=review_data_chunk_count,
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
    messy_relation = match_result._messy_relation
    if messy_relation is None:
        raise ValueError("The retained messy relation is unavailable for export.")
    available_record_count = int(messy_relation.count("*").fetchone()[0])
    expected_row_count = validate_record_selection_arguments(
        total_records_to_export=total_records_to_export,
        review_data_chunk_count=review_data_chunk_count,
        available_record_count=available_record_count,
    )

    bundle_id = str(uuid.uuid4())
    created_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
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
        total_records_to_export=total_records_to_export,
    )

    temporary_path = Path(
        tempfile.mkdtemp(
            prefix=f".{output_path.name}.tmp-",
            dir=output_path.parent,
        )
    )
    try:
        review_data_paths, parquet_validation = _write_review_data(
            match_result=match_result,
            review_relation=review_relation,
            temporary_path=temporary_path,
            review_data_chunk_count=review_data_chunk_count,
            expected_row_count=expected_row_count,
        )
        canonical_relation = match_result._canonical_relation
        if canonical_relation is None:
            raise ValueError("The retained canonical relation is unavailable for export.")
        _write_parquet(
            match_result,
            match_result.con.sql(
                f"""
                SELECT
                    CAST(ukam_address_id AS VARCHAR) AS ukam_address_id,
                    CAST(unique_id AS VARCHAR) AS unique_id,
                    original_address_concat::VARCHAR AS original_address_concat,
                    clean_full_address::VARCHAR AS clean_full_address,
                    postcode::VARCHAR AS postcode
                FROM ({canonical_relation.sql_query()})
                """
            ),
            temporary_path / _CANONICAL_DATA_FILE,
        )
        manifest = build_manifest(
            match_result=match_result,
            bundle_id=bundle_id,
            created_at_utc=created_at_utc,
            uk_address_matcher_version=__version__,
            parquet_validation=parquet_validation,
            top_n_candidates=top_n_candidates,
            canonical_label_column=_CANONICAL_LABEL_COLUMN,
            canonical_data_file=_CANONICAL_DATA_FILE,
            messy_columns=_MESSY_COLUMNS,
            canonical_columns=_CANONICAL_COLUMNS,
            data_files=tuple(path.name for path in review_data_paths),
            total_records_to_export=total_records_to_export,
            review_data_chunk_count=review_data_chunk_count,
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


def _write_review_data(
    *,
    match_result: MatchResult,
    review_relation: object,
    temporary_path: Path,
    review_data_chunk_count: int,
    expected_row_count: int,
) -> tuple[tuple[Path, ...], dict[str, object]]:
    if review_data_chunk_count == 1:
        paths = (temporary_path / "review_data.parquet",)
        _write_parquet(match_result, review_relation, paths[0])
    else:
        paths = tuple(
            temporary_path / f"review_data_chunk_{index + 1:03d}.parquet"
            for index in range(review_data_chunk_count)
        )
        ranked_sql = f"""
            SELECT * EXCLUDE (__ukam_export_row_number)
            FROM (
                SELECT
                    review_data.*,
                    NTILE({review_data_chunk_count}) OVER (ORDER BY unique_id)
                        AS __ukam_export_row_number
                FROM ({review_relation.sql_query()}) AS review_data
            ) AS ranked_review_data
            WHERE __ukam_export_row_number = {{chunk_index}}
            ORDER BY unique_id
        """
        for index, path in enumerate(paths):
            chunk_relation = match_result.con.sql(
                ranked_sql.format(chunk_index=index + 1)
            )
            _write_parquet(match_result, chunk_relation, path)

    validations = [
        validate_written_parquet(path, expected_row_count=expected_chunk_row_count)
        for path, expected_chunk_row_count in zip(
            paths,
            _chunk_row_counts(expected_row_count, review_data_chunk_count),
            strict=True,
        )
    ]
    return paths, _combine_parquet_validations(validations)


def _chunk_row_counts(row_count: int, chunk_count: int) -> tuple[int, ...]:
    base_count, remainder = divmod(row_count, chunk_count)
    return tuple(base_count + int(index < remainder) for index in range(chunk_count))


def _combine_parquet_validations(
    validations: list[dict[str, object]],
) -> dict[str, object]:
    first = validations[0]
    return {
        "schema": first["schema"],
        "row_count": sum(int(item["row_count"]) for item in validations),
        "matched_row_count": sum(int(item["matched_row_count"]) for item in validations),
        "unmatched_row_count": sum(
            int(item["unmatched_row_count"]) for item in validations
        ),
        "rows_with_candidates": sum(
            int(item["rows_with_candidates"]) for item in validations
        ),
        "rows_with_existing_labels": sum(
            int(item["rows_with_existing_labels"]) for item in validations
        ),
    }


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
