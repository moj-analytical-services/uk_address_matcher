from __future__ import annotations

import logging

from uk_address_matcher.logging.progress import ProgressMode, _ProgressBar

logger = logging.getLogger("uk_address_matcher")


def _format_elapsed(elapsed_seconds: float) -> str:
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds:02d}s"


def _format_elapsed_brief(elapsed_seconds: float) -> str:
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    if total_seconds < 60:
        return f"{total_seconds}s"
    return _format_elapsed(elapsed_seconds)


def log_stage_start(
    stage_label: str,
    total_records: int,
    total_chunks: int,
    *,
    progress_mode: ProgressMode,
) -> None:
    """Log the start of a chunked preparation stage."""
    if progress_mode == "off":
        return

    logger.info(
        "%s: %s records across %s chunk%s",
        stage_label,
        f"{total_records:,}",
        total_chunks,
        "" if total_chunks == 1 else "s",
    )


def log_stage_complete(
    stage_label: str,
    total_records: int,
    elapsed_seconds: float,
    *,
    progress_mode: ProgressMode,
) -> None:
    """Log completion of a chunked preparation stage."""
    if progress_mode == "off":
        return

    logger.info(
        "%s completed: %s records in %s",
        stage_label,
        f"{total_records:,}",
        _format_elapsed_brief(elapsed_seconds),
    )


def log_chunk_progress(
    total_records: int,
    processed_records: int,
    stage_label: str,
    *,
    progress_mode: ProgressMode,
    progress: _ProgressBar | None = None,
    chunk_index: int | None = None,
    total_chunks: int | None = None,
    chunk_elapsed_seconds: float | None = None,
) -> None:
    """Log chunk progress and separate it from an active live display."""
    if progress_mode != "auto":
        return

    if progress is not None:
        ensure_line_break = getattr(progress, "ensure_line_break", None)
        if callable(ensure_line_break):
            ensure_line_break()

    chunk_position = "?/?"
    if chunk_index is not None and total_chunks is not None:
        chunk_position = f"{chunk_index + 1}/{total_chunks}"

    elapsed_suffix = ""
    if chunk_elapsed_seconds is not None:
        elapsed_suffix = f", elapsed={_format_elapsed_brief(chunk_elapsed_seconds)}"

    logger.debug(
        "%s: chunk %s, %s/%s records%s",
        stage_label,
        chunk_position,
        f"{processed_records:,}",
        f"{total_records:,}",
        elapsed_suffix,
    )
