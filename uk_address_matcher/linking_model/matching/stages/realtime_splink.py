from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from uk_address_matcher.cleaning.steps.inverted_index import (
    BIGRAM_STRATEGY,
    TRIGRAM_STRATEGY,
)
from uk_address_matcher.linking_model.matching.stages.splink import SplinkStage

if TYPE_CHECKING:
    import duckdb

    from uk_address_matcher.sql_pipeline.runner import DebugOptions


@dataclass(repr=False)
class _RealTimeSplinkStage(SplinkStage):
    """Splink stage that pre-filters canonical rows through the realtime index.

    The stage still delegates scoring and candidate ranking to Splink
    ``predict()``. Its only extra responsibility is using the ART-indexed
    inverted index to materialise a much smaller canonical relation before the
    linker is built.
    """

    realtime_inverted_index_hashed: duckdb.DuckDBPyRelation | None = field(
        default=None,
        repr=False,
    )

    filtered_canonical_table: str | None = field(default=None, init=False, repr=False)
    candidate_pairs_table: str | None = field(default=None, init=False, repr=False)

    def find_matches(
        self,
        con: duckdb.DuckDBPyConnection,
        stage_name: str,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
        debug_options: Optional[DebugOptions] = None,
        explain: bool = False,
    ) -> Optional[duckdb.DuckDBPyRelation]:
        if explain:
            return None

        if self.realtime_inverted_index_hashed is None:
            raise ValueError(
                "RealTimeSplinkStage requires an ART-indexed inverted index. "
                "Use RealTimeAddressMatcher with a folder created by "
                "prepare_canonical_folder_for_realtime()."
            )

        filtered_canonical = self._materialise_filtered_canonical(
            con,
            df_unmatched=df_unmatched,
            df_canonical=df_canonical,
        )
        if filtered_canonical is None:
            return None

        return super().find_matches(
            con=con,
            stage_name=stage_name,
            df_unmatched=df_unmatched,
            df_canonical=filtered_canonical,
            debug_options=debug_options,
            explain=explain,
        )

    def _materialise_filtered_canonical(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        df_unmatched: duckdb.DuckDBPyRelation,
        df_canonical: duckdb.DuckDBPyRelation,
    ) -> duckdb.DuckDBPyRelation | None:
        from uk_address_matcher.sql_pipeline.helpers import _uid

        unmatched_count = df_unmatched.count("*").fetchone()[0]
        if unmatched_count == 0:
            return None

        candidate_pairs_table = f"__ukam__tmp_realtime_candidate_pairs__{_uid()}"
        filtered_table = f"__ukam__tmp_realtime_filtered_canonical__{_uid()}"

        unmatched_query = df_unmatched.sql_query()
        canonical_query = df_canonical.sql_query()
        hashed_index_query = self.realtime_inverted_index_hashed.sql_query()

        union_parts = []
        for strategy in (TRIGRAM_STRATEGY, BIGRAM_STRATEGY):
            union_parts.append(
                "SELECT unique_id AS messy_unique_id, "
                f"unnest({strategy.keys_sql_expr}) AS key "
                "FROM base"
            )
        unnested_keys_sql = " UNION ALL ".join(union_parts)

        con.execute(f"""
            CREATE TEMP TABLE {candidate_pairs_table} AS
            WITH base AS (
                SELECT
                    unique_id,
                    clean_full_address,
                    string_split(clean_full_address, ' ') AS __tokens
                FROM ({unmatched_query})
            ),
            messy_keys AS (
                SELECT DISTINCT
                    messy_unique_id,
                    hash(key) AS key_hash
                FROM ({unnested_keys_sql})
                WHERE key IS NOT NULL
            )
            SELECT DISTINCT
                messy_keys.messy_unique_id,
                ii.unique_id AS canonical_unique_id
            FROM messy_keys
            INNER JOIN ({hashed_index_query}) AS ii
                ON messy_keys.key_hash = ii.key_hash
        """)
        self.candidate_pairs_table = candidate_pairs_table

        candidate_count = con.table(candidate_pairs_table).count("*").fetchone()[0]
        if candidate_count == 0:
            return None

        con.execute(f"""
            CREATE TEMP TABLE {filtered_table} AS
            SELECT DISTINCT canonical.*
            FROM ({canonical_query}) AS canonical
            INNER JOIN (
                SELECT DISTINCT canonical_unique_id
                FROM {candidate_pairs_table}
            ) AS candidate_ids
                ON canonical.unique_id = candidate_ids.canonical_unique_id
        """)
        self.filtered_canonical_table = filtered_table

        canonical_count = con.table(filtered_table).count("*").fetchone()[0]
        if canonical_count == 0:
            return None

        return con.table(filtered_table)
