from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

from uk_address_matcher.logging.progress import ProgressMode, _ProgressBar

logger = logging.getLogger("uk_address_matcher")


def _format_elapsed(elapsed_seconds: float) -> str:
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds:02d}s"


def _format_elapsed_brief(elapsed_seconds: float) -> str:
    if 0 < elapsed_seconds < 1:
        return "<1s"
    total_seconds = int(round(max(0.0, elapsed_seconds)))
    if total_seconds < 60:
        return f"{total_seconds}s"
    return _format_elapsed(elapsed_seconds)


def _matching_stage_label(stage_name: str) -> str:
    return {
        "exact_matches": "Exact",
        "peeled_address": "Peeled address",
        "splink": "Splink",
        "unique_trigram": "Unique trigram",
    }.get(stage_name, stage_name.replace("_", " ").capitalize())


@contextmanager
def matching_stage_logging() -> Iterator[None]:
    def debug_timing(record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if record.levelno == logging.INFO and message.startswith(
            ("Blocking time:", "Predict time:")
        ):
            logger.debug("%s", message)
            return False
        return True

    timing_loggers = [
        logging.getLogger("splink.internals.blocking"),
        logging.getLogger("splink.internals.linker_components.inference"),
    ]
    for timing_logger in timing_loggers:
        timing_logger.addFilter(debug_timing)
    try:
        yield
    finally:
        for timing_logger in timing_loggers:
            timing_logger.removeFilter(debug_timing)


def log_matching_start(
    total_records: int,
    total_chunks: int,
    stage_names: list[str],
    *,
    progress_mode: ProgressMode,
) -> None:
    if progress_mode == "off":
        return
    chunk_summary = ""
    if total_chunks > 1:
        chunk_summary = (
            f" | {total_chunks} chunks"
            f" (~{total_records / total_chunks:,.0f} records/chunk)"
        )
    logger.info(
        "Address matching started | %s records%s | Pipeline: %s",
        f"{total_records:,}",
        chunk_summary,
        " -> ".join(_matching_stage_label(name) for name in stage_names),
    )


def log_matching_chunk(
    total_records: int,
    processed_records: int,
    chunk_number: int,
    total_chunks: int,
    chunk_elapsed_seconds: float,
    elapsed_seconds: float,
    processing_elapsed_seconds: float,
    *,
    progress_mode: ProgressMode,
) -> None:
    if progress_mode == "off":
        return
    remaining_summary = ""
    if chunk_number >= 2 and 0 < processed_records < total_records:
        remaining = (
            processing_elapsed_seconds
            * (total_records - processed_records)
            / processed_records
        )
        if remaining >= 5:
            remaining_summary = f" | ~{_format_elapsed_brief(remaining)} remaining"
    logger.info(
        "Chunk %d/%d completed in %s | %.0f%% complete | Elapsed: %s%s",
        chunk_number,
        total_chunks,
        _format_elapsed_brief(chunk_elapsed_seconds),
        100 * processed_records / total_records if total_records else 100.0,
        _format_elapsed_brief(elapsed_seconds),
        remaining_summary,
    )


def log_matching_complete(
    total_records: int,
    matched_records: int,
    total_chunks: int,
    elapsed_seconds: float,
    *,
    progress_mode: ProgressMode,
) -> None:
    if progress_mode == "off":
        return
    chunk_summary = f" | {total_chunks} chunks" if total_chunks > 1 else ""
    throughput = total_records / elapsed_seconds if elapsed_seconds > 0 else 0.0
    match_rate = 100 * matched_records / total_records if total_records else 0.0
    logger.info(
        "Address matching completed | %s records%s | %s | ~%s records/s"
        " | Matched: %s | Unmatched: %s | Match rate: %.1f%%",
        f"{total_records:,}",
        chunk_summary,
        _format_elapsed_brief(elapsed_seconds),
        f"{throughput:,.0f}",
        f"{matched_records:,}",
        f"{total_records - matched_records:,}",
        match_rate,
    )


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

    chunk_summary = f" across {total_chunks} chunks" if total_chunks > 1 else ""
    logger.info("%s: %s records%s", stage_label, f"{total_records:,}", chunk_summary)


def log_stage_complete(
    stage_label: str,
    total_records: int,
    elapsed_seconds: float | None = None,
    *,
    progress_mode: ProgressMode,
) -> None:
    """Log completion of a chunked preparation stage."""
    if progress_mode == "off":
        return

    if elapsed_seconds is None:
        logger.info("%s completed: %s records", stage_label, f"{total_records:,}")
    else:
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
    if progress_mode != "auto" or total_chunks == 1:
        return

    if progress is not None and getattr(progress, "enabled", False):
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

    logger.info(
        "%s: chunk %s, %s/%s records%s",
        stage_label,
        chunk_position,
        f"{processed_records:,}",
        f"{total_records:,}",
        elapsed_suffix,
    )


def report_stage(
    stage_name: str,
    stage_rows: int,
    matched_rows: int,
    stage_elapsed: float | None,
    *,
    progress_mode: ProgressMode,
    chunk_number: int | None = None,
    total_chunks: int | None = None,
) -> None:
    if progress_mode == "off" or stage_elapsed is None:
        return
    chunk_summary = ""
    log = logger.info
    if chunk_number is not None and total_chunks is not None:
        chunk_summary = f"Chunk {chunk_number}/{total_chunks} | "
        log = logger.debug
    log(
        "%s%s matching completed in %.2fs | Matched: %s | Remaining: %s",
        chunk_summary,
        _matching_stage_label(stage_name),
        stage_elapsed,
        f"{matched_rows:,}",
        f"{stage_rows - matched_rows:,}",
    )
