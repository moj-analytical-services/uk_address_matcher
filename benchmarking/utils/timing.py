from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter
from typing import Dict, Generator


@contextmanager
def time_phase(
    timings: Dict[str, Dict[str, float]],
    variant: str,
    phase: str,
) -> Generator[None, None, None]:
    """Context manager that records elapsed seconds for a given phase."""

    start = perf_counter()
    try:
        yield
    finally:
        duration = perf_counter() - start
        timings.setdefault(variant, {})[phase] = duration


def format_timing_summary(timings: Dict[str, Dict[str, float]]) -> list[str]:
    """Render timing information ready for printing."""

    lines: list[str] = []
    for variant, phases in timings.items():
        ordered_phases = sorted(phases.items())
        phase_parts = [f"{name}={duration:.2f}" for name, duration in ordered_phases]
        phase_summary = ", ".join(phase_parts) if phase_parts else "no timings"
        lines.append(f" - {variant}: {phase_summary}")
    return lines
