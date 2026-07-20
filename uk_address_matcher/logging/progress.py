from __future__ import annotations

import os
import sys
import time
from typing import Literal, TypeAlias, cast

ProgressMode: TypeAlias = Literal["auto", "stages", "off"]

_PROGRESS_MODES = frozenset({"auto", "stages", "off"})
_NOTEBOOK_ENVIRONMENT_VARIABLES = (
    "DATABRICKS_RUNTIME_VERSION",
    "DATABRICKS_CLUSTER_ID",
    "DATABRICKS_HOST",
)


def resolve_progress_mode(
    progress: str = "auto",
) -> ProgressMode:
    """Validate progress output configuration."""
    if not isinstance(progress, str):
        raise TypeError("progress must be one of: 'auto', 'stages', or 'off'.")
    if progress not in _PROGRESS_MODES:
        raise ValueError("progress must be one of: 'auto', 'stages', or 'off'.")

    return cast(ProgressMode, progress)


def _is_notebook_environment() -> bool:
    """Return whether the current process is running in a notebook runtime."""
    if any(os.environ.get(name) for name in _NOTEBOOK_ENVIRONMENT_VARIABLES):
        return True

    ipython = sys.modules.get("IPython")
    get_ipython = getattr(ipython, "get_ipython", None)
    if not callable(get_ipython):
        return False

    try:
        shell = get_ipython()
    except Exception:
        return False

    shell_module = getattr(shell.__class__, "__module__", "") if shell else ""
    return shell_module.startswith(("ipykernel.", "google.colab."))


def _supports_live_progress(stream: object) -> bool:
    """Return whether a stream can safely render an overwriting progress bar."""
    isatty = getattr(stream, "isatty", None)
    if not callable(isatty):
        return False

    try:
        return bool(isatty()) and not _is_notebook_environment()
    except (OSError, ValueError):
        return False


class _ProgressBar:
    def __init__(
        self,
        *,
        label: str,
        total: int,
        total_units: int | None = None,
        enabled: bool = True,
        stream: object | None = None,
        width: int = 24,
    ) -> None:
        self.label = label
        self.total = max(0, total)
        self.total_units = max(1, total_units or 24)
        self.completed_units = 0
        self.current = 0
        self.stream = sys.stderr if stream is None else stream
        self.width = max(8, width)
        self.start_time = time.monotonic()
        self._rendered = False
        self._last_render_length = 0
        self._left_edge = "▕"
        self._right_edge = "▏"
        self._filled_glyph = "▮"
        self._empty_glyph = "▯"

        self.enabled = enabled and _supports_live_progress(self.stream)
        self._configure_glyphs()

    def _configure_glyphs(self) -> None:
        encoding = getattr(self.stream, "encoding", None) or "utf-8"
        try:
            "▕▏▮▯".encode(encoding)
        except (LookupError, UnicodeEncodeError):
            self._left_edge = "["
            self._right_edge = "]"
            self._filled_glyph = "#"
            self._empty_glyph = "-"

    def _disable(self) -> None:
        self.enabled = False

    def update(self, current: int, *, completed_units: int | None = None) -> None:
        if not self.enabled:
            return

        self.current = min(max(0, current), self.total)
        if completed_units is not None:
            self.completed_units = min(max(0, completed_units), self.total_units)

        fraction = self.current / self.total if self.total else 1.0
        percent = int(fraction * 100)
        elapsed = time.monotonic() - self.start_time
        unit_fraction = (
            self.completed_units / self.total_units if self.total_units else 1.0
        )
        filled_units = int(round(self.width * unit_fraction))
        filled_units = min(max(0, filled_units), self.width)
        empty_units = self.width - filled_units
        bar = self._filled_glyph * filled_units + self._empty_glyph * empty_units

        try:
            line = (
                f"{self.label}: {percent:3d}% "
                f"{self._left_edge}{bar}{self._right_edge} "
                f"({self.current:,}/{self.total:,} records, "
                f"{elapsed:05.2f}s elapsed)"
            )
            padding = " " * max(0, self._last_render_length - len(line))
            self.stream.write(f"\r{line}{padding}")
            self.stream.flush()
            self._rendered = True
            self._last_render_length = len(line)
        except (OSError, UnicodeEncodeError, ValueError):
            self._disable()

    def ensure_line_break(self) -> None:
        if not self.enabled or not self._rendered:
            return

        try:
            self.stream.write("\n")
            self.stream.flush()
            self._rendered = False
            self._last_render_length = 0
        except (OSError, UnicodeEncodeError, ValueError):
            self._disable()

    def close(self) -> None:
        if self.enabled and self._rendered:
            try:
                self.stream.write("\n")
                self.stream.flush()
            except (OSError, UnicodeEncodeError, ValueError):
                self._disable()
