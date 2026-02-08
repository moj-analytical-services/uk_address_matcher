from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(frozen=True)
class TrigramStage(MatchingStage):
    """Trigram-based matching with uniqueness constraint."""

    ngram_size: int = 3
    min_unique_hits: int = 1
    include_conflicts: bool = False
    include_trigram_text: bool = True

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        from uk_address_matcher.linking_model.matching.input_filters import (
            _restrict_canonical_to_fuzzy_postcodes,
        )
        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[
                _restrict_canonical_to_fuzzy_postcodes("exact"),
                _resolve_with_trigrams(
                    ngram_size=self.ngram_size,
                    min_unique_hits=self.min_unique_hits,
                    include_conflicts=self.include_conflicts,
                    include_trigram_text=self.include_trigram_text,
                ),
            ],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
        )


# ---------------------------------------------------------------------------
# SQL implementation
# ---------------------------------------------------------------------------


def _ngram_expression(tokens_column: str, ngram_size: int) -> str:
    if ngram_size <= 0:
        raise ValueError("n must be greater than zero.")
    return f"""
        list_transform(
            range(1, length({tokens_column}) - {ngram_size} + 2),  -- stop is exclusive
            i -> {tokens_column}[i : i + {ngram_size} - 1]
        )
    """.strip()


def _trigram_hash_expression(alias: str = "tri") -> str:
    return f"hash(array_to_string({alias}, ' '))"


@pipeline_stage(
    name="resolve_with_trigrams",
    description="Resolve records using unique trigram matches",
    tags=["phase_1", "trigram", "exact_matching"],
)
def _resolve_with_trigrams(
    ngram_size: int = 3,
    min_unique_hits: int = 1,
    include_conflicts: bool = False,
    include_trigram_text: bool = False,
) -> list[CTEStep]:
    trigram_value = MatchReason.UNIQUE_TRIGRAM.value
    enum_values = str(MatchReason.enum_values())

    trigram_text_projection = (
        ", array_to_string(tri, ' ') AS trigram_text" if include_trigram_text else ""
    )
    candidate_text_projection = ", fuzzy.trigram_text" if include_trigram_text else ""
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
            trigram.{unit_fields.replace(chr(10), " ")},
            {_trigram_hash_expression()} AS trigram_hash
        FROM {{canonical_trigrams}} AS trigram,
        UNNEST(trigram.ngrams) AS u(tri)
        WHERE tri IS NOT NULL
    """

    unique_trigram_index_sql = """
        SELECT
            postcode,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            trigram_hash,
            MIN(canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(canonical_unique_id) AS canonical_unique_id
        FROM {canonical_trigrams_exploded}
        GROUP BY
            postcode,
            numeric_tokens,
            has_flat_indicator,
            flat_positional,
            flat_letter,
            flat_number,
            has_business_unit,
            business_unit_type,
            business_unit_id,
            trigram_hash
        HAVING COUNT(DISTINCT canonical_ukam_address_id) = 1
    """

    fuzzy_trigrams_sql = f"""
        SELECT
            f.ukam_address_id AS fuzzy_ukam_address_id,
            f.postcode,
            f.numeric_tokens,
            f.{unit_fields.replace(chr(10), " ")},
            {_ngram_expression("f.address_tokens", ngram_size)} AS ngrams
        FROM {{fuzzy_addresses}} AS f
        WHERE length(f.address_tokens) >= {ngram_size}
    """

    fuzzy_trigrams_exploded_sql = f"""
        SELECT DISTINCT
            fuzzy_trigrams.fuzzy_ukam_address_id,
            fuzzy_trigrams.postcode,
            fuzzy_trigrams.numeric_tokens,
            fuzzy_trigrams.{unit_fields.replace(chr(10), " ")},
            {_trigram_hash_expression()} AS trigram_hash
            {trigram_text_projection}
        FROM {{fuzzy_trigrams}} AS fuzzy_trigrams,
        UNNEST(fuzzy_trigrams.ngrams) AS u(tri)
        WHERE tri IS NOT NULL
    """

    trigram_candidate_links_sql = f"""
        SELECT
            fuzzy.fuzzy_ukam_address_id,
            fuzzy.postcode,
            unique_index.canonical_ukam_address_id,
            unique_index.canonical_unique_id,
            fuzzy.trigram_hash
            {candidate_text_projection}
        FROM {{fuzzy_trigrams_exploded}} AS fuzzy
        JOIN {{unique_trigram_index}} AS unique_index
            ON fuzzy.postcode = unique_index.postcode
            AND fuzzy.numeric_tokens = unique_index.numeric_tokens
            AND fuzzy.trigram_hash = unique_index.trigram_hash
            AND fuzzy.has_flat_indicator IS NOT DISTINCT FROM unique_index.has_flat_indicator
            AND fuzzy.flat_positional IS NOT DISTINCT FROM unique_index.flat_positional
            AND fuzzy.flat_letter IS NOT DISTINCT FROM unique_index.flat_letter
            AND fuzzy.flat_number IS NOT DISTINCT FROM unique_index.flat_number
            AND fuzzy.has_business_unit IS NOT DISTINCT FROM unique_index.has_business_unit
            AND fuzzy.business_unit_type IS NOT DISTINCT FROM unique_index.business_unit_type
            AND fuzzy.business_unit_id IS NOT DISTINCT FROM unique_index.business_unit_id
    """

    trigram_one_to_one_links_sql = f"""
        SELECT
            links.fuzzy_ukam_address_id,
            MIN(links.canonical_ukam_address_id) AS canonical_ukam_address_id,
            MIN(links.canonical_unique_id) AS resolved_canonical_id,
            links.postcode,
            COUNT(*) AS trigram_hit_count,
            LIST(DISTINCT links.trigram_hash) AS supporting_trigram_hashes
            {supporting_text_projection}
        FROM {{trigram_candidate_links}} AS links
        GROUP BY links.fuzzy_ukam_address_id, links.postcode
        HAVING COUNT(DISTINCT links.canonical_ukam_address_id) = 1
           AND COUNT(*) >= {min_unique_hits}
    """

    trigram_matches_sql = f"""
        SELECT
            fuzzy_ukam_address_id as ukam_address_id,
            canonical_ukam_address_id,
            resolved_canonical_id,
            trigram_hit_count,
            supporting_trigram_hashes,
            '{trigram_value}'::ENUM {enum_values} AS match_reason
            {supporting_text_select}
        FROM {{trigram_one_to_one_links}}
    """

    steps: list[CTEStep] = [
        CTEStep("canonical_trigrams", canonical_trigrams_sql),
        CTEStep("canonical_trigrams_exploded", canonical_trigrams_exploded_sql),
        CTEStep("unique_trigram_index", unique_trigram_index_sql),
        CTEStep("fuzzy_trigrams", fuzzy_trigrams_sql),
        CTEStep("fuzzy_trigrams_exploded", fuzzy_trigrams_exploded_sql),
        CTEStep("trigram_candidate_links", trigram_candidate_links_sql),
        CTEStep("trigram_one_to_one_links", trigram_one_to_one_links_sql),
        CTEStep("trigram_matches", trigram_matches_sql),
    ]

    if include_conflicts:
        trigram_conflicts_sql = f"""
            SELECT
                links.fuzzy_ukam_address_id,
                links.postcode,
                COUNT(DISTINCT links.canonical_ukam_address_id) AS candidate_canonical_count,
                LIST(DISTINCT links.canonical_ukam_address_id) AS candidate_canonical_ukam_address_ids,
                LIST(DISTINCT links.trigram_hash) AS conflicting_trigram_hashes
                {conflicts_text_projection}
            FROM {{trigram_candidate_links}} AS links
            GROUP BY links.fuzzy_ukam_address_id, links.postcode
            HAVING COUNT(DISTINCT links.canonical_ukam_address_id) > 1
        """
        steps.append(CTEStep("trigram_conflicts", trigram_conflicts_sql))

    return steps
