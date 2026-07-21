from __future__ import annotations

from io import StringIO

import pytest

from uk_address_matcher.logging.progress import _ProgressBar, resolve_progress_mode


class _TtyStringIO(StringIO):
    def isatty(self) -> bool:
        return True


def test_progress_bar_does_not_render_to_non_tty_stream() -> None:
    progress = _ProgressBar(label="Stage", total=10, stream=StringIO())

    progress.update(5, completed_units=1)

    assert progress.enabled is False


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
    with pytest.raises(ValueError, match="show_progress must be a boolean"):
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
