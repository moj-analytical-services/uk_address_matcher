from __future__ import annotations

from io import StringIO

import pytest

from uk_address_matcher.logging.chunking import (
    log_chunk_progress,
    log_matching_chunk,
    log_matching_complete,
    log_matching_start,
)
from uk_address_matcher.logging.progress import _ProgressBar, resolve_progress_mode


class _TtyStringIO(StringIO):
    def isatty(self) -> bool:
        return True


def test_progress_bar_does_not_render_to_non_tty_stream() -> None:
    progress = _ProgressBar(label="Stage", total=10, stream=StringIO())

    progress.update(5, completed_units=1)

    assert progress.enabled is False


def test_progress_bar_shows_remaining_time() -> None:
    stream = _TtyStringIO()
    progress = _ProgressBar(label="Matching", total=100, total_units=4, stream=stream)

    progress.update(25, completed_units=1, remaining_seconds=90)
    progress.close()

    assert "25%" in stream.getvalue()
    assert "25/100 records" in stream.getvalue()
    assert "0:01:30 remaining" in stream.getvalue()


def test_progress_bar_does_not_render_in_databricks_runtime(monkeypatch) -> None:
    stream = _TtyStringIO()
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "16.4")
    progress = _ProgressBar(label="Stage", total=10, stream=stream)

    progress.update(5, completed_units=1)

    assert progress.enabled is False
    assert stream.getvalue() == ""


@pytest.mark.parametrize(
    ("show_progress", "expected_mode"),
    [(True, "auto"), (False, "off"), ("auto", "auto"), ("stages", "stages")],
)
def test_resolve_progress_mode_accepts_boolean_and_named_values(
    show_progress: bool | str,
    expected_mode: str,
) -> None:
    assert resolve_progress_mode(show_progress) == expected_mode


def test_resolve_progress_mode_rejects_unknown_value() -> None:
    with pytest.raises(ValueError, match="show_progress must be one of"):
        resolve_progress_mode("verbose")


def test_progress_bar_ensure_line_break_flushes_active_render() -> None:
    stream = _TtyStringIO()
    progress = _ProgressBar(label="Stage", total=10, stream=stream)

    progress.update(5, completed_units=1)
    progress.ensure_line_break()

    assert stream.getvalue().endswith("\n")
    assert progress._rendered is False


def test_progress_bar_close_does_not_duplicate_newline_after_line_break() -> None:
    stream = _TtyStringIO()
    progress = _ProgressBar(label="Stage", total=10, stream=stream)

    progress.update(5, completed_units=1)
    progress.ensure_line_break()
    progress.close()

    assert stream.getvalue().count("\n") == 1


def test_auto_logs_each_chunk_when_live_progress_is_unavailable(caplog) -> None:
    caplog.set_level("INFO", logger="uk_address_matcher")

    log_chunk_progress(
        total_records=100,
        processed_records=25,
        stage_label="Cleaning",
        progress_mode="auto",
        chunk_index=0,
        total_chunks=4,
    )

    assert "Cleaning: chunk 1/4, 25/100 records" in caplog.text


def test_auto_keeps_live_progress_bar_without_chunk_logs(caplog) -> None:
    stream = _TtyStringIO()
    progress = _ProgressBar(label="Cleaning", total=100, stream=stream)
    progress.update(25, completed_units=1)
    caplog.set_level("DEBUG", logger="uk_address_matcher")

    log_chunk_progress(
        total_records=100,
        processed_records=25,
        stage_label="Cleaning",
        progress_mode="auto",
        progress=progress,
        chunk_index=0,
        total_chunks=4,
    )

    assert "chunk 1/4" not in caplog.text
    assert "\n" not in stream.getvalue()


@pytest.mark.parametrize("progress_mode", ["stages", "off"])
def test_less_verbose_modes_suppress_chunk_logs(caplog, progress_mode) -> None:
    caplog.set_level("DEBUG", logger="uk_address_matcher")

    log_chunk_progress(
        total_records=100,
        processed_records=25,
        stage_label="Cleaning",
        progress_mode=progress_mode,
        chunk_index=0,
        total_chunks=4,
    )

    assert "chunk 1/4" not in caplog.text


@pytest.mark.parametrize("total_chunks", [1, 5])
def test_matching_job_summaries(caplog, total_chunks) -> None:
    caplog.set_level("INFO", logger="uk_address_matcher")
    log_matching_start(
        348687,
        total_chunks,
        ["exact_matches", "peeled_address", "splink"],
        progress_mode="auto",
    )
    log_matching_complete(348687, 345564, total_chunks, 83.4, progress_mode="auto")

    start, finish = caplog.messages
    assert start.startswith("Address matching started | 348,687 records")
    assert start.endswith("Pipeline: Exact -> Peeled address -> Splink")
    assert "1m 23s | ~4,181 records/s" in finish
    assert "Matched: 345,564 | Unmatched: 3,123 | Match rate: 99.1%" in finish
    if total_chunks > 1:
        assert "5 chunks (~69,737 records/chunk)" in start
        assert "5 chunks" in finish
    else:
        assert "chunk" not in start + finish


def test_matching_progress_uses_completed_records_and_delays_eta(caplog) -> None:
    caplog.set_level("INFO", logger="uk_address_matcher")
    for chunk_number, processed in enumerate([2, 5, 9], start=1):
        elapsed = 5 * chunk_number
        log_matching_chunk(
            9, processed, chunk_number, 3, 5, elapsed, elapsed, progress_mode="auto"
        )

    first, second, final = caplog.messages
    assert first == "Chunk 1/3 completed in 5s | 22% complete | Elapsed: 5s"
    assert second == (
        "Chunk 2/3 completed in 5s | 56% complete | Elapsed: 10s | ~8s remaining"
    )
    assert final == "Chunk 3/3 completed in 5s | 100% complete | Elapsed: 15s"
    log_matching_chunk(10, 9, 2, 3, 1, 44.1, 44.1, progress_mode="auto")
    assert "remaining" not in caplog.messages[-1]
    log_matching_chunk(10, 9, 2, 3, 1, 45, 45, progress_mode="auto")
    assert caplog.messages[-1].endswith("| ~5s remaining")
