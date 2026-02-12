from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

from uk_address_matcher.linking_model.matching.input_filters import (
    _restrict_canonical_to_messy_postcodes,
)
from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
    run_sql_pipeline,
)
from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


UniqueTrigramScope = Literal["postcode", "global"]


@dataclass(frozen=True)
class UniqueTrigramStage(MatchingStage):
    """Match unresolved records using unique trigram evidence."""

    ngram_size: int = 3
    min_unique_hits: int = 1
    include_conflicts: bool = False
    include_trigram_text: bool = True
    unique_scope: UniqueTrigramScope = "postcode"

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_messy_postcodes("exact")
                if self.unique_scope == "postcode"
                else _project_canonical_for_trigrams(),
                _resolve_with_trigrams(
                    ngram_size=self.ngram_size,
                    min_unique_hits=self.min_unique_hits,
                    include_conflicts=self.include_conflicts,
                    include_trigram_text=self.include_trigram_text,
                    unique_scope=self.unique_scope,
                ),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )


@pipeline_stage(
    name="project_canonical_for_trigrams",
    description=(
        "Project canonical fields for trigram matching without postcode restriction."
    ),
    tags=["phase_1", "matching", "utility"],
    stage_output="canonical_addresses_restricted",
)
def _project_canonical_for_trigrams() -> list[CTEStep]:
    canonical_select_fields = [
        "canon.clean_full_address",
        "canon.postcode",
        "canon.unique_id AS canonical_unique_id",
        "canon.ukam_address_id AS ukam_address_id",
        "canon.address_tokens",
        "canon.numeric_tokens",
        "canon.unusual_tokens_arr",
        "canon.has_flat_indicator",
        "canon.flat_positional",
        "canon.flat_letter",
        "canon.flat_number",
        "canon.has_business_unit",
        "canon.business_unit_type",
        "canon.business_unit_id",
    ]

    canonical_select_fields_str = ",\n            ".join(canonical_select_fields)
    sql = f"""
        SELECT
            {canonical_select_fields_str}
        FROM {{canonical_addresses}} AS canon
        WHERE canon.unique_id IS NOT NULL
    """
    return [CTEStep("canonical_addresses_restricted", sql)]


def _ngram_expression(tokens_column: str, ngram_size: int) -> str:
    """Generate SQL expression for creating n-grams from token arrays."""
    if ngram_size <= 0:
        raise ValueError("n must be greater than zero.")
    return f"""
        list_transform(
            range(1, length({tokens_column}) - {ngram_size} + 2),
            i -> {tokens_column}[i : i + {ngram_size} - 1]
        )
    """.strip()


def _trigram_hash_expression(alias: str = "tri") -> str:
    """Generate SQL expression for hashing trigrams."""
    return f"hash(array_to_string({alias}, ' '))"


@pipeline_stage(
    name="resolve_with_trigrams",
    description="Resolve records using unique trigram matches",
    tags=["phase_1", "trigram", "matching"],
)
def _resolve_with_trigrams(
    ngram_size: int = 3,
    min_unique_hits: int = 1,
    include_conflicts: bool = False,
    include_trigram_text: bool = False,
    unique_scope: UniqueTrigramScope = "postcode",
) -> list[CTEStep]:
    """Match records using unique trigrams that identify a single canonical address.

    This stage generates trigrams (3-token sequences) from both messy and canonical
    addresses, then matches based on trigrams that uniquely identify a single
    canonical address within the same postcode.

    Numeric/unit tokens are only used as a verification step after a trigram has been
    confirmed unique within the postcode.
    """
    trigram_value = MatchReason.UNIQUE_TRIGRAM.value
    enum_values = str(MatchReason.enum_values())

    if unique_scope not in ("postcode", "global"):
        raise ValueError(
            f"unique_scope must be 'postcode' or 'global'. Got '{unique_scope}'."
        )

    trigram_text_projection = (
        ", array_to_string(tri, ' ') AS trigram_text" if include_trigram_text else ""
    )
    candidate_text_projection = ", messy.trigram_text" if include_trigram_text else ""
    supporting_text_projection = (
        ", LIST(DISTINCT links.trigram_text) AS supporting_trigram_texts"
        if include_trigram_text
        else ""
    )
    conflicts_text_projection = (
        ", LIST(DISTINCT links.trigram_text) AS conflicting_trigram_texts"
        if include_trigram_text
        else ""
    )
    supporting_text_select = (
        ", supporting_trigram_texts" if include_trigram_text else ""
    )

    unit_fields = """
        has_flat_indicator,
        flat_positional,
        flat_letter,
        flat_number,
        has_business_unit,
        business_unit_type,
        business_unit_id
    """

    canonical_trigrams_sql = f"""
        SELECT
            canon.ukam_address_id as canonical_ukam_address_id,
            canon.canonical_unique_id,
            canon.postcode,
            canon.numeric_tokens,
            canon.unusual_tokens_arr,
            canon.{unit_fields.replace(chr(10), " ")},
            {_ngram_expression("canon.address_tokens", ngram_size)} AS ngrams
        FROM {{canonical_addresses_restricted}} AS canon
        WHERE length(canon.address_tokens) >= {ngram_size}
    """

    canonical_trigrams_exploded_sql = f"""
        SELECT DISTINCT
            trigram.canonical_ukam_address_id,
            trigram.canonical_unique_id,
            trigram.postcode,
            trigram.numeric_tokens,
            trigram.unusual_tokens_arr,
            trigram.{unit_fields.replace(chr(10), " ")},
            {_trigram_hash_expression()} AS trigram_hash
        FROM {{canonical_trigrams}} AS trigram,
        UNNEST(trigram.ngrams) AS u(tri)
        WHERE tri IS NOT NULL
    """

    count_select = (
        "postcode, trigram_hash" if unique_scope == "postcode" else "trigram_hash"
    )
    trigram_postcode_counts_sql = f"""
    SELECT
        {count_select},
        COUNT(DISTINCT canonical_unique_id) AS canonical_unique_id_count
    FROM {{canonical_trigrams_exploded}}
    GROUP BY
        {count_select}
    """

    if unique_scope == "postcode":
        unique_join_condition = """
        ON ct.postcode = tpc.postcode
        AND ct.trigram_hash = tpc.trigram_hash
        """
    else:
        unique_join_condition = """
        ON ct.trigram_hash = tpc.trigram_hash
        """

    unique_trigram_index_sql = f"""
    SELECT
        ct.postcode,
        ct.trigram_hash,
        ct.canonical_ukam_address_id,
        ct.canonical_unique_id,
        ct.numeric_tokens,
        ct.unusual_tokens_arr,
        ct.has_flat_indicator,
        ct.flat_positional,
        ct.flat_letter,
        ct.flat_number,
        ct.has_business_unit,
        ct.business_unit_type,
        ct.business_unit_id
    FROM {{canonical_trigrams_exploded}} AS ct
    JOIN {{trigram_postcode_counts}} AS tpc
        {unique_join_condition}
    WHERE tpc.canonical_unique_id_count = 1
    """

    messy_trigrams_sql = f"""
        SELECT
            m.ukam_address_id AS messy_ukam_address_id,
            m.postcode,
            m.numeric_tokens,
            m.unusual_tokens_arr,
            m.{unit_fields.replace(chr(10), " ")},
            {_ngram_expression("m.address_tokens", ngram_size)} AS ngrams
        FROM {{messy_addresses}} AS m
        WHERE length(m.address_tokens) >= {ngram_size}
    """

    messy_trigrams_exploded_sql = f"""
        SELECT DISTINCT
            messy_trigrams.messy_ukam_address_id,
            messy_trigrams.postcode,
            messy_trigrams.numeric_tokens,
            messy_trigrams.unusual_tokens_arr,
            messy_trigrams.{unit_fields.replace(chr(10), " ")},
            {_trigram_hash_expression()} AS trigram_hash
            {trigram_text_projection}
        FROM {{messy_trigrams}} AS messy_trigrams,
        UNNEST(messy_trigrams.ngrams) AS u(tri)
        WHERE tri IS NOT NULL
    """

    postcode_join = (
        "AND messy.postcode = unique_index.postcode"
        if unique_scope == "postcode"
        else ""
    )

    trigram_candidate_links_sql = f"""
        SELECT
            messy.messy_ukam_address_id,
            messy.postcode,
            unique_index.canonical_ukam_address_id,
            unique_index.canonical_unique_id,
            messy.trigram_hash,
            messy.numeric_tokens AS messy_numeric_tokens,
            unique_index.numeric_tokens AS canonical_numeric_tokens,
            messy.unusual_tokens_arr AS messy_unusual_tokens_arr,
            unique_index.unusual_tokens_arr AS canonical_unusual_tokens_arr,
            messy.has_flat_indicator AS messy_has_flat_indicator,
            unique_index.has_flat_indicator AS canonical_has_flat_indicator,
            messy.flat_positional AS messy_flat_positional,
            unique_index.flat_positional AS canonical_flat_positional,
            messy.flat_letter AS messy_flat_letter,
            unique_index.flat_letter AS canonical_flat_letter,
            messy.flat_number AS messy_flat_number,
            unique_index.flat_number AS canonical_flat_number,
            messy.has_business_unit AS messy_has_business_unit,
            unique_index.has_business_unit AS canonical_has_business_unit,
            messy.business_unit_type AS messy_business_unit_type,
            unique_index.business_unit_type AS canonical_business_unit_type,
            messy.business_unit_id AS messy_business_unit_id,
            unique_index.business_unit_id AS canonical_business_unit_id,
            CASE
                WHEN messy.has_flat_indicator IS TRUE THEN
                    unique_index.has_flat_indicator IS TRUE
                    AND (
                        COALESCE(messy.flat_positional, '') = COALESCE(unique_index.flat_positional, '')
                        AND COALESCE(messy.flat_letter, '') = COALESCE(unique_index.flat_letter, '')
                        AND COALESCE(messy.flat_number, '') = COALESCE(unique_index.flat_number, '')
                    )
                ELSE TRUE
            END AS flat_ok,
            CASE
                WHEN messy.has_business_unit IS TRUE THEN
                    unique_index.has_business_unit IS TRUE
                    AND (
                        messy.business_unit_type IS NULL
                        OR messy.business_unit_type = unique_index.business_unit_type
                    )
                    AND (
                        messy.business_unit_id IS NULL
                        OR messy.business_unit_id = unique_index.business_unit_id
                    )
                ELSE TRUE
            END AS business_ok,
            CASE
                WHEN COALESCE(length(messy.numeric_tokens), 0) = 0 THEN TRUE
                ELSE list_contains(unique_index.numeric_tokens, messy.numeric_tokens[1])
            END AS number_ok,
            CASE
                WHEN COALESCE(length(messy.unusual_tokens_arr), 0) = 0 THEN TRUE
                ELSE (
                    COALESCE(
                        length(
                            list_filter(
                                messy.unusual_tokens_arr,
                                x -> list_contains(unique_index.unusual_tokens_arr, x)
                            )
                        ),
                        0
                    ) > 0
                )
            END AS unusual_overlap_ok
            {candidate_text_projection}
        FROM {{messy_trigrams_exploded}} AS messy
        JOIN {{unique_trigram_index}} AS unique_index
            ON messy.numeric_tokens = unique_index.numeric_tokens
            {postcode_join}
            AND messy.trigram_hash = unique_index.trigram_hash
            AND messy.has_flat_indicator IS NOT DISTINCT FROM unique_index.has_flat_indicator
            AND messy.flat_positional IS NOT DISTINCT FROM unique_index.flat_positional
            AND messy.flat_letter IS NOT DISTINCT FROM unique_index.flat_letter
            AND messy.flat_number IS NOT DISTINCT FROM unique_index.flat_number
            AND messy.has_business_unit IS NOT DISTINCT FROM unique_index.has_business_unit
            AND messy.business_unit_type IS NOT DISTINCT FROM unique_index.business_unit_type
            AND messy.business_unit_id IS NOT DISTINCT FROM unique_index.business_unit_id
        WHERE (
            CASE
                WHEN messy.has_flat_indicator IS TRUE THEN
                    unique_index.has_flat_indicator IS TRUE
                    AND (
                        COALESCE(messy.flat_positional, '') = COALESCE(unique_index.flat_positional, '')
                        AND COALESCE(messy.flat_letter, '') = COALESCE(unique_index.flat_letter, '')
                        AND COALESCE(messy.flat_number, '') = COALESCE(unique_index.flat_number, '')
                    )
                ELSE TRUE
            END
        )
        AND (
            CASE
                WHEN messy.has_business_unit IS TRUE THEN
                    unique_index.has_business_unit IS TRUE
                    AND (
                        messy.business_unit_type IS NULL
                        OR messy.business_unit_type = unique_index.business_unit_type
                    )
                    AND (
                        messy.business_unit_id IS NULL
                        OR messy.business_unit_id = unique_index.business_unit_id
                    )
                ELSE TRUE
            END
        )
        AND (
            CASE
                WHEN COALESCE(length(messy.numeric_tokens), 0) = 0 THEN TRUE
                ELSE list_contains(unique_index.numeric_tokens, messy.numeric_tokens[1])
            END
        )
        AND (
            CASE
                WHEN COALESCE(length(messy.unusual_tokens_arr), 0) = 0 THEN TRUE
                ELSE (
                    COALESCE(
                        length(
                            list_filter(
                                messy.unusual_tokens_arr,
                                x -> list_contains(unique_index.unusual_tokens_arr, x)
                            )
                        ),
                        0
                    ) > 0
                )
            END
        )
    """

    trigram_one_to_one_links_sql = f"""
        SELECT
            links.messy_ukam_address_id,
            MIN(links.canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(links.canonical_unique_id) AS resolved_canonical_id,
            links.postcode,
            COUNT(DISTINCT links.trigram_hash) AS trigram_hit_count,
            LIST(DISTINCT links.trigram_hash) AS supporting_trigram_hashes
            {supporting_text_projection},
            MIN(links.flat_ok) AS flat_ok,
            MIN(links.number_ok) AS number_ok,
            MIN(links.business_ok) AS business_ok,
            MIN(links.unusual_overlap_ok) AS unusual_overlap_ok,
            MIN(links.flat_ok AND links.number_ok AND links.business_ok AND links.unusual_overlap_ok)
                AS contradictions_passed
        FROM {{trigram_candidate_links}} AS links
        GROUP BY links.messy_ukam_address_id, links.postcode
        HAVING COUNT(DISTINCT links.canonical_unique_id) = 1
           AND COUNT(DISTINCT links.trigram_hash) >= {min_unique_hits}
    """

    trigram_matches_sql = f"""
        SELECT
            messy_ukam_address_id as ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            trigram_hit_count,
            supporting_trigram_hashes,
            '{trigram_value}'::ENUM {enum_values} AS match_reason
            {supporting_text_select},
            flat_ok,
            number_ok,
            business_ok,
            unusual_overlap_ok,
            contradictions_passed
        FROM {{trigram_one_to_one_links}}
    """

    steps: list[CTEStep] = [
        CTEStep("canonical_trigrams", canonical_trigrams_sql),
        CTEStep("canonical_trigrams_exploded", canonical_trigrams_exploded_sql),
        CTEStep("trigram_postcode_counts", trigram_postcode_counts_sql),
        CTEStep("unique_trigram_index", unique_trigram_index_sql),
        CTEStep("messy_trigrams", messy_trigrams_sql),
        CTEStep("messy_trigrams_exploded", messy_trigrams_exploded_sql),
        CTEStep("trigram_candidate_links", trigram_candidate_links_sql),
        CTEStep("trigram_one_to_one_links", trigram_one_to_one_links_sql),
        CTEStep("trigram_matches", trigram_matches_sql),
    ]

    if include_conflicts:
        trigram_conflicts_sql = f"""
            SELECT
                links.messy_ukam_address_id,
                links.postcode,
                COUNT(DISTINCT links.canonical_unique_id) AS candidate_canonical_count,
                LIST(DISTINCT links.canonical_ukam_address_id) AS candidate_canonical_ukam_address_ids,
                LIST(DISTINCT links.trigram_hash) AS conflicting_trigram_hashes
                {conflicts_text_projection}
            FROM {{trigram_candidate_links}} AS links
            GROUP BY links.messy_ukam_address_id, links.postcode
            HAVING COUNT(DISTINCT links.canonical_unique_id) > 1
        """
        steps.append(CTEStep("trigram_conflicts", trigram_conflicts_sql))

    return steps
