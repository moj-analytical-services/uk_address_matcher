from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter


@contextmanager
def time_phase(
    timings: dict[str, dict[str, float]],
    label: str,
    phase: str,
) -> Iterator[None]:
    phase_timings = timings.setdefault(label, {})
    start = perf_counter()
    try:
        yield
    finally:
        phase_timings[phase] = perf_counter() - start
