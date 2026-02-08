"""Legacy deterministic matching API (removed).

The old wrappers have been retired in favour of ``run_matching()``. Any code
that still imports this module should be updated to call the unified orchestrator.
"""

from __future__ import annotations

raise ImportError(
    "Legacy matching helpers have been removed. Use run_matching() instead."
)
