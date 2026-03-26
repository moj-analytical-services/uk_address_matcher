from __future__ import annotations

from benchmarking.utils.io import apply_env_from_private_config, get_env_setting

apply_env_from_private_config()

SAMPLE_MODE = False
CANONICAL_PATH = get_env_setting("UKAM_OS_CANONICAL_PREPARED")

APPLY_CANONICAL_FILTER = False
# CANONICAL_FILTER_SQL: str | None = None
CANONICAL_FILTER_SQL = (
    "substr(classificationcode, 1, 1) = 'R' or substr(classificationcode, 2, 1) = 'C'"
)
