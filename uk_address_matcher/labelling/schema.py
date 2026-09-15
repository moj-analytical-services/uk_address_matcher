from __future__ import annotations

DEFAULT_LABELLING_BUNDLE_DIRECTORY = "ukam_labelling_bundle"
DEFAULT_TOP_N_CANDIDATES = 3
MAX_TOP_N_CANDIDATES = 10

REQUIRED_TOP_LEVEL_COLUMNS = (
    "bundle_id",
    "uk_address_matcher_version",
    "created_at_utc",
    "unique_id",
    "messy_address",
    "messy_cleaned_address",
    "messy_postcode",
    "ukam_label",
    "has_existing_label",
    "ukam_label_clean_full_address",
    "ukam_label_postcode",
    "resolved_canonical_id",
    "resolved_label_id",
    "resolved_canonical_address",
    "resolved_canonical_postcode",
    "match_reason",
    "match_stage",
    "is_matched",
    "match_weight",
    "distinguishability",
    "candidate_count",
    "top_candidates",
)


def quote_identifier(identifier: str) -> str:
    """Return a DuckDB identifier quoted safely for generated SQL."""
    return '"' + identifier.replace('"', '""') + '"'
