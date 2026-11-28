from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.linking_model.matching.stages.base_stage import MatchingStage
from uk_address_matcher.sql_pipeline.match_reasons import MatchReason
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


_BLANK_POSTCODE = "nullif(trim(postcode), '') IS NULL"


@pipeline_stage(
    name="postcode_free_matches",
    description="Match unique cleaned addresses without a postcode",
)
def _postcode_free_matches() -> list[CTEStep]:
    return [
        CTEStep(
            "postcode_free_matches",
            f"""
            WITH blank AS MATERIALIZED (
                SELECT ukam_address_id, clean_full_address
                FROM {{__ukam__tmp_messy_addresses}}
                WHERE {_BLANK_POSTCODE}
            ), full_requests AS MATERIALIZED (
                SELECT DISTINCT clean_full_address
                FROM blank
            ), full_aliases AS MATERIALIZED (
                SELECT canonical.unique_id, canonical.ukam_address_id,
                       requested.clean_full_address
                FROM {{__ukam__tmp_canonical_addresses}} canonical
                JOIN full_requests requested
                  ON canonical.clean_full_address = requested.clean_full_address
                WHERE len(canonical.clean_full_address_tokens) >= 4
                  AND len(canonical.numeric_tokens) > 0
            ), full_keys AS (
                SELECT clean_full_address, min(unique_id) AS resolved_canonical_id,
                       min(ukam_address_id) AS canonical_ukam_address_id
                FROM full_aliases
                GROUP BY clean_full_address
                HAVING count(DISTINCT unique_id) = 1
            )
            SELECT blank.ukam_address_id, full_keys.resolved_canonical_id,
                   full_keys.canonical_ukam_address_id,
                   '{MatchReason.POSTCODE_FREE_FULL.value}' AS match_reason,
                   NULL::DOUBLE AS match_weight,
                   NULL::DOUBLE AS distinguishability
            FROM blank
            JOIN full_keys USING (clean_full_address)
            """,
        )
    ]


@dataclass(frozen=True, repr=False)
class PostcodeFreeMatchStage(MatchingStage):
    """Match blank-postcode inputs through unique full cleaned addresses.

    Place after ordinary exact matching and before Splink. Only keys requested
    by the current chunk are resolved, without a prepared lookup.
    """

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        if not explain and not df_unmatched.filter(_BLANK_POSTCODE).limit(1).fetchall():
            return None

        from uk_address_matcher.linking_model.matching.stages._sql_helpers import (
            run_sql_pipeline,
        )

        return run_sql_pipeline(
            con=con,
            pipeline_stages=[_postcode_free_matches()],
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
            debug_options=debug_options,
            explain=explain,
        )
