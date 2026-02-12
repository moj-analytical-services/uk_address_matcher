from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional, Sequence

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(frozen=True)
class SignatureTemplate:
    """Definition of a single signature strategy.

    Each template produces a ``VARCHAR`` signature value per row.  Values are
    scoped to a postcode: a signature is considered *unique* when it maps to
    exactly one canonical ``unique_id`` for a given postcode.

    Attributes:
        name: Short identifier used in provenance columns and diagnostics.
        priority: Lower values = evaluated first / preferred in tie-breaks.
        sig_value_sql: SQL expression returning a ``VARCHAR``.  Use ``{t}`` as
            the table alias placeholder (resolved at render time).
        filter_sql: SQL ``WHERE`` predicate ensuring the signature is
            well-formed.  Use ``{t}`` for the alias.  Rows failing the
            predicate are silently excluded.
    """

    name: str
    priority: int
    sig_value_sql: str
    filter_sql: str


# Regex to extract the first alphabetic word after the last number in an
# address.  Greedy ``.*`` advances past all earlier numbers so the captured
# group is always the word immediately following the final numeric token.
# Example: "FLAT 2 100 HIGH STREET" → "HIGH" (not "FLAT").
_WORD_AFTER_LAST_NUMBER_RE = r".*\d\S*\s+([A-Z]+)"

_STOP_WORDS = (
    "FLAT",
    "UNIT",
    "ROOM",
    "APARTMENT",
    "BLOCK",
    "FLOOR",
    "GROUND",
    "FIRST",
    "SECOND",
    "THIRD",
    "FOURTH",
    "FIFTH",
    "SIXTH",
    "SEVENTH",
    "EIGHTH",
    "NINTH",
    "TENTH",
    "TOP",
    "BASEMENT",
    "LOWER",
    "UPPER",
    "REAR",
    "FRONT",
)
_STOP_WORDS_SQL = ", ".join(f"'{word}'" for word in _STOP_WORDS)

_FLAT_BUSINESS_TOKENS = (
    "FLAT",
    "UNIT",
    "ROOM",
    "APARTMENT",
    "OFFICE",
    "SUITE",
    "SHOP",
    "STUDIO",
)
_FLAT_BUSINESS_GUARD_SQL = " OR ".join(
    f"list_contains({{t}}.address_tokens, '{word}')" for word in _FLAT_BUSINESS_TOKENS
)

_RANGE_MARKER_RE = r"\d+\s*(?:-|TO)\s*\d+"

POSTCODE_NUM_FLAT = SignatureTemplate(
    name="postcode_num_flat",
    priority=1,
    sig_value_sql=(
        "CONCAT_WS('|', {t}.numeric_token_1, {t}.numeric_token_2, {t}.flat_identity)"
    ),
    filter_sql=(
        "{t}.numeric_token_1 IS NOT NULL "
        "AND {t}.numeric_token_2 IS NOT NULL "
        "AND {t}.flat_identity IS NOT NULL "
        "AND ({t}.flat_number IS NOT NULL OR {t}.flat_letter IS NOT NULL) "
        "AND ({t}.has_business_unit IS NULL OR {t}.has_business_unit = false) "
        "AND NOT REGEXP_MATCHES({t}.clean_full_address, '" + _RANGE_MARKER_RE + "')"
    ),
)

POSTCODE_NUM = SignatureTemplate(
    name="postcode_num",
    priority=2,
    sig_value_sql=(
        "CONCAT_WS('|', {t}.numeric_token_1, "
        "REGEXP_EXTRACT({t}.clean_full_address, '"
        + _WORD_AFTER_LAST_NUMBER_RE
        + "', 1))"
    ),
    filter_sql=(
        "{t}.numeric_token_1 IS NOT NULL "
        "AND ({t}.has_flat_indicator IS NULL OR {t}.has_flat_indicator = false) "
        "AND ({t}.has_business_unit IS NULL OR {t}.has_business_unit = false) "
        "AND NOT (" + _FLAT_BUSINESS_GUARD_SQL + ") "
        "AND NOT REGEXP_MATCHES({t}.clean_full_address, '" + _RANGE_MARKER_RE + "') "
        "AND REGEXP_EXTRACT({t}.clean_full_address, '"
        + _WORD_AFTER_LAST_NUMBER_RE
        + "', 1) IS NOT NULL "
        "AND REGEXP_EXTRACT({t}.clean_full_address, '"
        + _WORD_AFTER_LAST_NUMBER_RE
        + "', 1) != '' "
        "AND UPPER(REGEXP_EXTRACT({t}.clean_full_address, '"
        + _WORD_AFTER_LAST_NUMBER_RE
        + "', 1)) NOT IN ("
        + _STOP_WORDS_SQL
        + ")"
    ),
)

POSTCODE_NUMS_FIRST_WORD = SignatureTemplate(
    name="postcode_nums_first_word",
    priority=3,
    sig_value_sql=(
        "CONCAT_WS('|', array_to_string({t}.numeric_tokens, ' '), "
        "TRIM(SPLIT_PART({t}.address_without_numbers, ' ', 1)))"
    ),
    filter_sql=(
        "{t}.numeric_tokens IS NOT NULL "
        "AND length({t}.numeric_tokens) > 0 "
        "AND {t}.address_without_numbers IS NOT NULL "
        "AND TRIM({t}.address_without_numbers) != '' "
        "AND UPPER(TRIM(SPLIT_PART({t}.address_without_numbers, ' ', 1))) "
        "NOT IN (" + _STOP_WORDS_SQL + ")"
    ),
)

NUM1_NUM2 = SignatureTemplate(
    name="num1_num2",
    priority=4,
    sig_value_sql="CONCAT_WS('|', {t}.numeric_token_1, {t}.numeric_token_2)",
    filter_sql=("{t}.numeric_token_1 IS NOT NULL AND {t}.numeric_token_2 IS NOT NULL"),
)

NUM_BUSINESS_UNIT = SignatureTemplate(
    name="num_business_unit",
    priority=5,
    sig_value_sql=(
        "CONCAT_WS('|', {t}.numeric_token_1, "
        "{t}.business_unit_type, {t}.business_unit_id)"
    ),
    filter_sql=(
        "{t}.numeric_token_1 IS NOT NULL "
        "AND {t}.has_business_unit = true "
        "AND {t}.business_unit_id IS NOT NULL"
    ),
)

NUM_FLAT_POSITIONAL = SignatureTemplate(
    name="num_flat_positional",
    priority=6,
    sig_value_sql="CONCAT_WS('|', {t}.numeric_token_1, {t}.flat_positional)",
    filter_sql=(
        "{t}.numeric_token_1 IS NOT NULL "
        "AND {t}.flat_positional IS NOT NULL "
        "AND {t}.flat_positional != ''"
    ),
)

DEFAULT_SIGNATURE_TEMPLATES: tuple[SignatureTemplate, ...] = (
    POSTCODE_NUM_FLAT,
    POSTCODE_NUM,
    POSTCODE_NUMS_FIRST_WORD,
    NUM1_NUM2,
)


@pipeline_stage(
    name="restrict_canonical_for_signatures",
    description=(
        "Restrict canonical addresses to postcodes observed in the messy input, "
        "projecting additional columns needed by signature matching."
    ),
    tags=["phase_1", "matching", "utility"],
    stage_output="canonical_addresses_restricted",
)
def _restrict_canonical_for_signatures() -> list[CTEStep]:
    """Filter canonical by postcode and project signature-relevant columns."""
    canonical_select_fields = [
        "canon.clean_full_address",
        "canon.postcode",
        "canon.unique_id AS canonical_unique_id",
        "canon.ukam_address_id AS ukam_address_id",
        "canon.address_tokens",
        "canon.numeric_tokens",
        "canon.has_flat_indicator",
        "canon.flat_positional",
        "canon.flat_letter",
        "canon.flat_number",
        "canon.flat_identity",
        "canon.has_business_unit",
        "canon.business_unit_type",
        "canon.business_unit_id",
        "canon.numeric_token_1",
        "canon.numeric_token_2",
        "canon.numeric_token_3",
        "canon.address_without_numbers",
        "canon.unusual_tokens_arr",
    ]
    fields_str = ",\n            ".join(canonical_select_fields)

    sql = f"""
        SELECT
            {fields_str}
        FROM {{canonical_addresses}} AS canon
        JOIN (
            SELECT DISTINCT postcode AS postcode_key
            FROM {{messy_addresses}}
            WHERE postcode IS NOT NULL
        ) AS messy
          ON canon.postcode = messy.postcode_key
        WHERE canon.unique_id IS NOT NULL
    """
    return [CTEStep("canonical_addresses_restricted", sql)]


def _build_union_sql(
    templates: Sequence[SignatureTemplate],
    source_placeholder: str,
    id_column: str,
    ukam_id_column: str,
) -> str:
    """Build a UNION ALL query across all signature templates.

    Parameters
    ----------
    templates:
        Ordered signature templates.
    source_placeholder:
        Pipeline placeholder for the source table (e.g. ``{canonical_addresses_restricted}``).
    id_column:
        Column used as the user-facing unique identifier.
    ukam_id_column:
        Column used as the internal ``ukam_address_id``.
    """
    parts: list[str] = []
    for tmpl in templates:
        alias = "t"
        sig_value = tmpl.sig_value_sql.replace("{t}", alias)
        filter_expr = tmpl.filter_sql.replace("{t}", alias)
        part = f"""
        SELECT
            {alias}.{id_column} AS record_id,
            {alias}.{ukam_id_column} AS ukam_address_id,
            {alias}.postcode,
            '{tmpl.name}' AS sig_type,
            {tmpl.priority} AS sig_priority,
            ({sig_value})::VARCHAR AS sig_value
        FROM {source_placeholder} AS {alias}
        WHERE {filter_expr}
          AND ({sig_value}) IS NOT NULL
        """
        parts.append(part.strip())
    return "\nUNION ALL\n".join(parts)


@pipeline_stage(
    name="resolve_with_signatures",
    description="Resolve records using unique postcode-scoped signatures",
    tags=["phase_1", "signature", "matching"],
)
def _resolve_with_signatures(
    templates: Sequence[SignatureTemplate] = DEFAULT_SIGNATURE_TEMPLATES,
    include_provenance: bool = True,
) -> list[CTEStep]:
    """Match records using postcode-scoped signatures unique in canonical data.

    The approach:

    1.  Generate signatures for every canonical and messy row from multiple
        templates (``UNION ALL``).
    2.  Keep only canonical signatures that are unique within ``(postcode,
        sig_type, sig_value)`` — a signature appearing for 2+ canonical
        records is discarded.
    3.  Join messy signatures to the unique canonical index on
        ``(postcode, sig_type, sig_value)``.
    4.  For each messy record pick the highest-priority (lowest
        ``sig_priority``) match, discarding ties that point to different
        canonical records.
    """
    sig_value = MatchReason.UNIQUE_SIGNATURE.value
    enum_values = str(MatchReason.enum_values())

    # Canonical signatures (all templates, UNION ALL)
    canonical_sigs_sql = _build_union_sql(
        templates,
        source_placeholder="{canonical_addresses_restricted}",
        id_column="canonical_unique_id",
        ukam_id_column="ukam_address_id",
    )

    # Keep only signatures unique to one canonical record per postcode
    unique_canonical_sigs_sql = """
        SELECT
            postcode,
            sig_type,
            sig_priority,
            sig_value,
            ANY_VALUE(record_id) AS canonical_unique_id,
            ANY_VALUE(ukam_address_id) AS canonical_ukam_address_id
        FROM {canonical_signatures}
        GROUP BY postcode, sig_type, sig_value, sig_priority
        HAVING COUNT(DISTINCT record_id) = 1
    """

    # Messy signatures (same templates)
    messy_sigs_sql = _build_union_sql(
        templates,
        source_placeholder="{messy_addresses}",
        id_column="unique_id",
        ukam_id_column="ukam_address_id",
    )

    # Candidate links: join messy ↔ unique canonical
    candidate_links_sql = """
        SELECT
            messy.ukam_address_id AS messy_ukam_address_id,
            messy.postcode,
            messy.sig_type,
            messy.sig_priority,
            messy.sig_value,
            canon.canonical_unique_id,
            canon.canonical_ukam_address_id,
            messy_addr.numeric_tokens AS messy_numeric_tokens,
            canon_addr.numeric_tokens AS canonical_numeric_tokens,
            messy_addr.numeric_token_1 AS messy_numeric_token_1,
            messy_addr.numeric_token_2 AS messy_numeric_token_2,
            messy_addr.numeric_token_3 AS messy_numeric_token_3,
            messy_addr.has_flat_indicator AS messy_has_flat_indicator,
            canon_addr.has_flat_indicator AS canonical_has_flat_indicator,
            messy_addr.flat_identity AS messy_flat_identity,
            canon_addr.flat_identity AS canonical_flat_identity,
            messy_addr.flat_letter AS messy_flat_letter,
            canon_addr.flat_letter AS canonical_flat_letter,
            messy_addr.flat_number AS messy_flat_number,
            canon_addr.flat_number AS canonical_flat_number,
            messy_addr.flat_positional AS messy_flat_positional,
            canon_addr.flat_positional AS canonical_flat_positional,
            messy_addr.has_business_unit AS messy_has_business_unit,
            canon_addr.has_business_unit AS canonical_has_business_unit,
            messy_addr.business_unit_type AS messy_business_unit_type,
            canon_addr.business_unit_type AS canonical_business_unit_type,
            messy_addr.business_unit_id AS messy_business_unit_id,
            canon_addr.business_unit_id AS canonical_business_unit_id,
            messy_addr.unusual_tokens_arr AS messy_unusual_tokens_arr,
            canon_addr.unusual_tokens_arr AS canonical_unusual_tokens_arr,
            CASE
                WHEN messy_addr.has_flat_indicator IS TRUE THEN
                    canon_addr.has_flat_indicator IS TRUE
                    AND COALESCE(messy_addr.flat_identity, '') = COALESCE(canon_addr.flat_identity, '')
                ELSE TRUE
            END AS flat_ok,
            CASE
                WHEN messy_addr.has_business_unit IS TRUE THEN
                    canon_addr.has_business_unit IS TRUE
                    AND (
                        messy_addr.business_unit_type IS NULL
                        OR messy_addr.business_unit_type = canon_addr.business_unit_type
                    )
                    AND (
                        messy_addr.business_unit_id IS NULL
                        OR messy_addr.business_unit_id = canon_addr.business_unit_id
                    )
                ELSE TRUE
            END AS business_ok,
            CASE
                WHEN COALESCE(length(messy_addr.numeric_tokens), 0) = 0 THEN TRUE
                ELSE (
                    list_contains(canon_addr.numeric_tokens, messy_addr.numeric_token_1)
                    OR list_contains(canon_addr.numeric_tokens, messy_addr.numeric_token_2)
                    OR list_contains(canon_addr.numeric_tokens, messy_addr.numeric_token_3)
                )
            END AS number_ok,
            CASE
                WHEN COALESCE(length(messy_addr.unusual_tokens_arr), 0) = 0 THEN TRUE
                ELSE (
                    COALESCE(
                        length(
                            list_filter(
                                messy_addr.unusual_tokens_arr,
                                x -> list_contains(canon_addr.unusual_tokens_arr, x)
                            )
                        ),
                        0
                    ) > 0
                )
            END AS unusual_overlap_ok
        FROM {messy_signatures} AS messy
        INNER JOIN {unique_canonical_signatures} AS canon
            ON messy.postcode = canon.postcode
           AND messy.sig_type = canon.sig_type
           AND messy.sig_value = canon.sig_value
        INNER JOIN {messy_addresses} AS messy_addr
            ON messy.ukam_address_id = messy_addr.ukam_address_id
        INNER JOIN {canonical_addresses_restricted} AS canon_addr
            ON canon.canonical_ukam_address_id = canon_addr.ukam_address_id
        WHERE (
            CASE
                WHEN messy_addr.has_flat_indicator IS TRUE THEN
                    canon_addr.has_flat_indicator IS TRUE
                    AND COALESCE(messy_addr.flat_identity, '') = COALESCE(canon_addr.flat_identity, '')
                ELSE TRUE
            END
        )
        AND (
            CASE
                WHEN messy_addr.has_business_unit IS TRUE THEN
                    canon_addr.has_business_unit IS TRUE
                    AND (
                        messy_addr.business_unit_type IS NULL
                        OR messy_addr.business_unit_type = canon_addr.business_unit_type
                    )
                    AND (
                        messy_addr.business_unit_id IS NULL
                        OR messy_addr.business_unit_id = canon_addr.business_unit_id
                    )
                ELSE TRUE
            END
        )
        AND (
            CASE
                WHEN COALESCE(length(messy_addr.numeric_tokens), 0) = 0 THEN TRUE
                ELSE (
                    list_contains(canon_addr.numeric_tokens, messy_addr.numeric_token_1)
                    OR list_contains(canon_addr.numeric_tokens, messy_addr.numeric_token_2)
                    OR list_contains(canon_addr.numeric_tokens, messy_addr.numeric_token_3)
                )
            END
        )
        AND (
            CASE
                WHEN COALESCE(length(messy_addr.unusual_tokens_arr), 0) = 0 THEN TRUE
                ELSE (
                    COALESCE(
                        length(
                            list_filter(
                                messy_addr.unusual_tokens_arr,
                                x -> list_contains(canon_addr.unusual_tokens_arr, x)
                            )
                        ),
                        0
                    ) > 0
                )
            END
        )
    """

    # CTE 5 — resolve: discard same-priority conflicts, pick lowest priority
    # First eliminate any (messy, priority) groups that match >1 canonical record
    # then pick the lowest-priority surviving match per messy record.
    best_match_sql = """
        SELECT
            messy_ukam_address_id,
            MIN(canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(canonical_unique_id) AS resolved_canonical_id,
            MIN(sig_type) AS signature_type,
            MIN(sig_priority) AS sig_priority,
            MIN(flat_ok) AS flat_ok,
            MIN(number_ok) AS number_ok,
            MIN(business_ok) AS business_ok,
            MIN(unusual_overlap_ok) AS unusual_overlap_ok,
            MIN(flat_ok AND number_ok AND business_ok AND unusual_overlap_ok)
                AS contradictions_passed
        FROM (
            SELECT
                links.*,
                ROW_NUMBER() OVER (
                    PARTITION BY links.messy_ukam_address_id
                    ORDER BY links.sig_priority
                ) AS rn
            FROM {signature_candidate_links} AS links
            INNER JOIN (
                -- keep only (messy, priority) groups that agree on one canonical
                SELECT messy_ukam_address_id, sig_priority
                FROM {signature_candidate_links}
                GROUP BY messy_ukam_address_id, sig_priority
                HAVING COUNT(DISTINCT canonical_unique_id) = 1
            ) AS clean_groups
                ON links.messy_ukam_address_id = clean_groups.messy_ukam_address_id
               AND links.sig_priority = clean_groups.sig_priority
        ) AS ranked
        WHERE rn = 1
        GROUP BY messy_ukam_address_id
    """

    # CTE 6 — final matches formatted for the results table
    provenance_col = ", signature_type" if include_provenance else ""
    matches_sql = f"""
        SELECT
            messy_ukam_address_id AS ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            '{sig_value}'::ENUM {enum_values} AS match_reason
            {provenance_col},
            flat_ok,
            number_ok,
            business_ok,
            unusual_overlap_ok,
            contradictions_passed
        FROM {{signature_best_match}}
    """

    return [
        CTEStep("canonical_signatures", canonical_sigs_sql),
        CTEStep("unique_canonical_signatures", unique_canonical_sigs_sql),
        CTEStep("messy_signatures", messy_sigs_sql),
        CTEStep("signature_candidate_links", candidate_links_sql),
        CTEStep("signature_best_match", best_match_sql),
        CTEStep("signature_matches", matches_sql),
    ]


@dataclass(frozen=True)
class UniqueSignatureStage(MatchingStage):
    """Match unresolved records using unique postcode-scoped signatures.

    Signatures are compound deterministic keys built from postcode plus a small
    selection of cleaned token features (house number, flat identity, etc.).
    A match is accepted only when a signature maps to exactly one canonical
    record for the given postcode.

    Attributes:
        signature_templates: Ordered templates to evaluate. Defaults to
            :data:`DEFAULT_SIGNATURE_TEMPLATES`.
        include_provenance: If ``True`` the ``signature_type`` column is
            propagated to the results table indicating which template produced
            each match.
    """

    signature_templates: Sequence[SignatureTemplate] = field(
        default_factory=lambda: DEFAULT_SIGNATURE_TEMPLATES,
    )
    include_provenance: bool = True

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_for_signatures(),
                _resolve_with_signatures(
                    templates=self.signature_templates,
                    include_provenance=self.include_provenance,
                ),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )


__all__ = [
    "SignatureTemplate",
    "UniqueSignatureStage",
    "DEFAULT_SIGNATURE_TEMPLATES",
    "POSTCODE_NUM_FLAT",
    "POSTCODE_NUM",
    "POSTCODE_NUMS_FIRST_WORD",
    "NUM1_NUM2",
    "NUM_BUSINESS_UNIT",
    "NUM_FLAT_POSITIONAL",
]
