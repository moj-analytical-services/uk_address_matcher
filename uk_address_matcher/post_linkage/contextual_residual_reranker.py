from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection, DuckDBPyRelation

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    ContextualTelemetry = MutableMapping[str, float | int]


@dataclass(frozen=True)
class ContextualRerankerConfig:
    absolute_splink_floor: float = -20.0
    maximum_splink_gap: float = 12.0
    consensus_threshold: float = 0.60
    local_frequency_smoothing: float = 0.50
    candidate_support_weight: float = 2.25
    rival_omission_weight: float = 1.75
    contrast_weight: float = 0.25
    split_join_weight: float = 2.00
    soft_match_weight: float = 1.75
    contextual_multiplier: float = 1.75
    contextual_cap: float = 4.00
    minimum_support_advantage: float = 0.50
    minimum_candidate_support: float = 0.75
    evidence_sufficiency_target: float = 0.50
    soft_match_threshold: float = 0.90
    maximum_acceptance_lift: float = 2.00
    use_split_join: bool = True
    use_soft_matching: bool = True
    include_diagnostics: bool = False


PRECISION_K3_CONFIG = ContextualRerankerConfig()


def improve_predictions_using_contextual_residuals(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
    config: ContextualRerankerConfig = PRECISION_K3_CONFIG,
    contextual_base_score: str = "phase1_score",
    telemetry: ContextualTelemetry | None = None,
) -> DuckDBPyRelation:
    """
    The input is the output of ``improve_predictions_using_distinguishing_tokens``.
    Contextual evidence is centred within a raw-Splink-eligible candidate group
    and is never global IDF.
    """
    if contextual_base_score not in {"phase1_score", "splink_match_weight"}:
        raise ValueError(
            "contextual_base_score must be 'phase1_score' or 'splink_match_weight'"
        )

    absolute_splink_floor = config.absolute_splink_floor
    maximum_splink_gap = config.maximum_splink_gap
    consensus_threshold = config.consensus_threshold
    local_frequency_smoothing = config.local_frequency_smoothing
    candidate_support_weight = config.candidate_support_weight
    rival_omission_weight = config.rival_omission_weight
    contrast_weight = config.contrast_weight
    split_join_weight = config.split_join_weight
    soft_match_weight = config.soft_match_weight
    contextual_multiplier = config.contextual_multiplier
    contextual_cap = config.contextual_cap
    minimum_support_advantage = config.minimum_support_advantage
    minimum_candidate_support = config.minimum_candidate_support
    evidence_sufficiency_target = config.evidence_sufficiency_target
    soft_match_threshold = config.soft_match_threshold
    use_split_join = config.use_split_join
    use_soft_matching = config.use_soft_matching
    started_at = perf_counter()
    required_columns = {
        "unique_id_l",
        "unique_id_r",
        "ukam_address_id_l",
        "ukam_address_id_r",
        "tokens_l",
        "tokens_r",
        "splink_match_weight",
        "phase1_score",
    }
    missing_columns = sorted(required_columns.difference(df_predict.columns))
    if missing_columns:
        raise ValueError(
            "Contextual residual reranking requires columns: "
            + ", ".join(missing_columns)
        )

    tables = {
        "input": "__ukam__contextual_input",
        "candidate_scope": "__ukam__contextual_candidate_scope",
        "query_tokens_raw": "__ukam__contextual_query_tokens_raw",
        "query_tokens": "__ukam__contextual_query_tokens",
        "candidate_tokens_raw": "__ukam__contextual_candidate_tokens_raw",
        "candidate_tokens": "__ukam__contextual_candidate_tokens",
        "query_joined": "__ukam__contextual_query_joined_tokens",
        "candidate_joined": "__ukam__contextual_candidate_joined_tokens",
        "exact": "__ukam__contextual_exact_matches",
        "split_join": "__ukam__contextual_split_join_matches",
        "soft_candidates": "__ukam__contextual_soft_candidates",
        "soft_scored": "__ukam__contextual_soft_scored",
        "soft_qualified": "__ukam__contextual_soft_qualified",
        "soft": "__ukam__contextual_soft_matches",
        "matches": "__ukam__contextual_token_matches",
        "query_statistics": "__ukam__contextual_query_statistics",
        "candidate_statistics": "__ukam__contextual_candidate_statistics",
        "pair_strength": "__ukam__contextual_pair_query_strength",
        "pair_rival": "__ukam__contextual_pair_query_rival_strength",
        "candidate_support": "__ukam__contextual_candidate_support",
        "features": "__ukam__contextual_features",
        "scored": "__ukam__contextual_scored",
        "diagnostics": "__ukam__contextual_diagnostics",
        "final": "__ukam__contextual_final",
    }

    for table_name in tables.values():
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

    minimum_support_advantage_condition = "TRUE"
    if minimum_support_advantage is not None:
        minimum_support_advantage_condition = f"""
            scored.candidate_id = scored.reference_winner_candidate_id
            OR scored.candidate_residual_support
                - scored.reference_winner_residual_support >= {minimum_support_advantage}
        """
    minimum_candidate_support_condition = "TRUE"
    if minimum_candidate_support is not None:
        minimum_candidate_support_condition = (
            f"scored.candidate_residual_support >= {minimum_candidate_support}"
        )

    con.execute(
        f"CREATE TEMP TABLE {tables['input']} AS "
        f"SELECT * FROM ({df_predict.sql_query()}) AS predictions"
    )
    con.execute(f"""
        CREATE TEMP TABLE {tables["candidate_scope"]} AS
        WITH deduplicated AS (
            SELECT
                input.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ukam_address_id_r, ukam_address_id_l
                    ORDER BY {contextual_base_score} DESC, unique_id_l, unique_id_r
                ) AS pair_row_number
            FROM {tables["input"]} AS input
        ),
        raw_scope AS (
            SELECT
                * EXCLUDE (pair_row_number),
                MAX(splink_match_weight) OVER (
                    PARTITION BY ukam_address_id_r
                ) AS best_splink_match_weight
            FROM deduplicated
            WHERE pair_row_number = 1
        ),
        eligibility AS (
            SELECT
                *,
                splink_match_weight - best_splink_match_weight AS splink_gap_from_best,
                splink_match_weight >= {absolute_splink_floor}
                    AND splink_match_weight >=
                        best_splink_match_weight - {maximum_splink_gap}
                    AS is_contextually_eligible
            FROM raw_scope
        )
        SELECT
            *,
            COUNT(*) FILTER (WHERE is_contextually_eligible) OVER (
                PARTITION BY ukam_address_id_r
            ) AS eligible_candidate_count
        FROM eligibility
    """)

    token_class = """
        CASE
            WHEN regexp_full_match(token, '^[A-Z]+$') THEN 'alphabetic'
            WHEN regexp_full_match(token, '^[0-9]+$') THEN 'numeric'
            ELSE 'mixed_alphanumeric'
        END
    """
    con.execute(f"""
        CREATE TEMP TABLE {tables["query_tokens_raw"]} AS
        WITH source_lists AS (
            SELECT
                ukam_address_id_r AS query_id,
                tokens_r,
                ROW_NUMBER() OVER (
                    PARTITION BY ukam_address_id_r
                    ORDER BY unique_id_r, ukam_address_id_l
                ) AS source_row_number
            FROM {tables["candidate_scope"]}
        )
        SELECT
            source_lists.query_id,
            token AS query_token,
            token_position AS query_token_position,
            {token_class.replace("token", "token")} AS query_token_class
        FROM source_lists,
            UNNEST(tokens_r) WITH ORDINALITY AS unnested(token, token_position)
        WHERE source_row_number = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["query_tokens"]} AS
        SELECT
            query_id,
            query_token,
            MIN(query_token_position) AS query_token_position,
            ANY_VALUE(query_token_class) AS query_token_class
        FROM {tables["query_tokens_raw"]}
        GROUP BY query_id, query_token
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["candidate_tokens_raw"]} AS
        SELECT
            ukam_address_id_r AS query_id,
            ukam_address_id_l AS candidate_id,
            token AS candidate_token,
            token_position AS candidate_token_position,
            {token_class.replace("token", "token")} AS candidate_token_class,
            is_contextually_eligible
        FROM {tables["candidate_scope"]},
            UNNEST(tokens_l) WITH ORDINALITY AS unnested(token, token_position)
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["candidate_tokens"]} AS
        SELECT
            query_id,
            candidate_id,
            candidate_token,
            MIN(candidate_token_position) AS candidate_token_position,
            ANY_VALUE(candidate_token_class) AS candidate_token_class,
            ANY_VALUE(is_contextually_eligible) AS is_contextually_eligible
        FROM {tables["candidate_tokens_raw"]}
        GROUP BY query_id, candidate_id, candidate_token
    """)
    joined_class = """
        CASE
            WHEN regexp_full_match(joined_token, '^[A-Z]+$') THEN 'alphabetic'
            WHEN regexp_full_match(joined_token, '^[0-9]+$') THEN 'numeric'
            ELSE 'mixed_alphanumeric'
        END
    """
    con.execute(f"""
        CREATE TEMP TABLE {tables["query_joined"]} AS
        WITH adjacent AS (
            SELECT
                query_id,
                query_token_position AS first_position,
                LEAD(query_token_position) OVER source_window AS second_position,
                query_token AS first_token,
                LEAD(query_token) OVER source_window AS second_token,
                query_token_class AS first_token_class,
                LEAD(query_token_class) OVER source_window AS second_token_class
            FROM {tables["query_tokens_raw"]}
            WINDOW source_window AS (
                PARTITION BY query_id ORDER BY query_token_position
            )
        )
        SELECT
            query_id,
            first_position,
            second_position,
            first_token || second_token AS joined_token,
            {joined_class} AS joined_token_class,
            first_token_class,
            second_token_class
        FROM adjacent
        WHERE second_position = first_position + 1
          AND NOT (
              first_token_class = 'numeric' AND second_token_class = 'numeric'
          )
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["candidate_joined"]} AS
        WITH adjacent AS (
            SELECT
                query_id,
                candidate_id,
                candidate_token_position AS first_position,
                LEAD(candidate_token_position) OVER candidate_window AS second_position,
                candidate_token AS first_token,
                LEAD(candidate_token) OVER candidate_window AS second_token,
                candidate_token_class AS first_token_class,
                LEAD(candidate_token_class) OVER candidate_window AS second_token_class,
                is_contextually_eligible
            FROM {tables["candidate_tokens_raw"]}
            WINDOW candidate_window AS (
                PARTITION BY query_id, candidate_id ORDER BY candidate_token_position
            )
        )
        SELECT
            query_id,
            candidate_id,
            first_position,
            second_position,
            first_token || second_token AS joined_token,
            {joined_class} AS joined_token_class,
            first_token_class,
            second_token_class,
            is_contextually_eligible
        FROM adjacent
        WHERE second_position = first_position + 1
          AND NOT (
              first_token_class = 'numeric' AND second_token_class = 'numeric'
          )
    """)

    con.execute(f"""
        CREATE TEMP TABLE {tables["exact"]} AS
        SELECT
            query.query_id,
            candidate.candidate_id,
            query.query_token,
            query.query_token_position,
            candidate.candidate_token,
            candidate.candidate_token_position,
            1.0::DOUBLE AS match_strength,
            'exact' AS match_mechanism,
            1 AS mechanism_precedence
        FROM {tables["query_tokens"]} AS query
        JOIN {tables["candidate_tokens"]} AS candidate
            ON candidate.query_id = query.query_id
           AND candidate.candidate_token = query.query_token
    """)

    if use_split_join:
        con.execute(f"""
            CREATE TEMP TABLE {tables["split_join"]} AS
            WITH proposals AS (
                SELECT
                    query.query_id,
                    joined.candidate_id,
                    query.query_token,
                    query.query_token_position,
                    joined.joined_token AS candidate_token,
                    joined.first_position AS candidate_token_position,
                    0.95::DOUBLE AS match_strength,
                    'split_join' AS match_mechanism,
                    2 AS mechanism_precedence
                FROM {tables["query_tokens"]} AS query
                JOIN {tables["candidate_joined"]} AS joined
                    ON joined.query_id = query.query_id
                   AND joined.joined_token = query.query_token
                   AND (
                       (query.query_token_class = 'alphabetic'
                        AND joined.first_token_class = 'alphabetic'
                        AND joined.second_token_class = 'alphabetic')
                       OR (query.query_token_class = 'mixed_alphanumeric'
                           AND (
                               (joined.first_token_class = 'alphabetic'
                                AND joined.second_token_class = 'numeric')
                               OR (joined.first_token_class = 'numeric'
                                   AND joined.second_token_class = 'alphabetic')
                           ))
                   )
                UNION ALL
                SELECT
                    joined.query_id,
                    candidate.candidate_id,
                    joined.joined_token AS query_token,
                    joined.first_position AS query_token_position,
                    candidate.candidate_token,
                    candidate.candidate_token_position,
                    0.95::DOUBLE AS match_strength,
                    'split_join' AS match_mechanism,
                    2 AS mechanism_precedence
                FROM {tables["query_joined"]} AS joined
                JOIN {tables["candidate_tokens"]} AS candidate
                    ON candidate.query_id = joined.query_id
                   AND candidate.candidate_token = joined.joined_token
                   AND (
                       (candidate.candidate_token_class = 'alphabetic'
                        AND joined.first_token_class = 'alphabetic'
                        AND joined.second_token_class = 'alphabetic')
                       OR (candidate.candidate_token_class = 'mixed_alphanumeric'
                           AND (
                               (joined.first_token_class = 'alphabetic'
                                AND joined.second_token_class = 'numeric')
                               OR (joined.first_token_class = 'numeric'
                                   AND joined.second_token_class = 'alphabetic')
                           ))
                   )
            ),
            residual_proposals AS (
                SELECT proposals.*
                FROM proposals
                LEFT JOIN {tables["exact"]} AS exact
                    ON exact.query_id = proposals.query_id
                   AND exact.candidate_id = proposals.candidate_id
                   AND exact.query_token_position = proposals.query_token_position
                WHERE exact.query_id IS NULL
            )
            SELECT *
            FROM residual_proposals
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY query_id, candidate_id, query_token_position
                ORDER BY candidate_token, candidate_token_position
            ) = 1
        """)
    else:
        con.execute(f"""
            CREATE TEMP TABLE {tables["split_join"]} AS
            SELECT * FROM {tables["exact"]} WHERE FALSE
        """)

    if use_soft_matching:
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft_candidates"]} AS
            WITH residual_query_tokens AS (
                SELECT query.*
                FROM {tables["query_tokens"]} AS query
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM (
                        SELECT query_id, query_token_position FROM {tables["exact"]}
                        UNION
                        SELECT query_id, query_token_position FROM {tables["split_join"]}
                    ) AS explained
                    WHERE explained.query_id = query.query_id
                      AND explained.query_token_position = query.query_token_position
                )
            ),
            residual_candidate_tokens AS (
                SELECT candidate.*
                FROM {tables["candidate_tokens"]} AS candidate
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM (
                        SELECT query_id, candidate_id, candidate_token_position
                        FROM {tables["exact"]}
                        UNION
                        SELECT query_id, candidate_id, candidate_token_position
                        FROM {tables["split_join"]}
                    ) AS explained
                    WHERE explained.query_id = candidate.query_id
                      AND explained.candidate_id = candidate.candidate_id
                                            AND explained.candidate_token_position =
                                                    candidate.candidate_token_position
                )
            )
            SELECT
                query.query_id,
                candidate.candidate_id,
                query.query_token,
                query.query_token_position,
                query.query_token_class,
                candidate.candidate_token,
                candidate.candidate_token_position,
                candidate.candidate_token_class
            FROM residual_query_tokens AS query
            JOIN residual_candidate_tokens AS candidate
                ON candidate.query_id = query.query_id
               AND query.query_token_class = 'alphabetic'
               AND candidate.candidate_token_class = 'alphabetic'
               AND length(query.query_token) >= 5
               AND length(candidate.candidate_token) >= 5
               AND abs(
                   length(query.query_token) - length(candidate.candidate_token)
               ) <= 3
        """)
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft_scored"]} AS
            SELECT
                query_id,
                candidate_id,
                query_token,
                query_token_position,
                candidate_token,
                candidate_token_position,
                jaro_winkler_similarity(
                    query_token, candidate_token
                ) AS match_strength
            FROM {tables["soft_candidates"]}
        """)
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft_qualified"]} AS
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY query_id, candidate_id, query_token_position
                    ORDER BY match_strength DESC, candidate_token,
                        candidate_token_position
                ) AS candidate_token_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY query_id, candidate_id, candidate_token_position
                    ORDER BY match_strength DESC, query_token, query_token_position
                ) AS query_token_rank
            FROM {tables["soft_scored"]}
            WHERE match_strength >= {soft_match_threshold}
        """)
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft"]} AS
            SELECT
                query_id,
                candidate_id,
                query_token,
                query_token_position,
                candidate_token,
                candidate_token_position,
                match_strength,
                'soft' AS match_mechanism,
                3 AS mechanism_precedence
            FROM {tables["soft_qualified"]}
            WHERE candidate_token_rank = 1
              AND query_token_rank = 1
        """)
    else:
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft_candidates"]} AS
            SELECT * FROM {tables["exact"]} WHERE FALSE
        """)
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft_scored"]} AS
            SELECT * FROM {tables["exact"]} WHERE FALSE
        """)
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft_qualified"]} AS
            SELECT * FROM {tables["exact"]} WHERE FALSE
        """)
        con.execute(f"""
            CREATE TEMP TABLE {tables["soft"]} AS
            SELECT * FROM {tables["exact"]} WHERE FALSE
        """)

    con.execute(f"""
        CREATE TEMP TABLE {tables["matches"]} AS
        SELECT *
        FROM (
            SELECT * FROM {tables["exact"]}
            UNION ALL
            SELECT * FROM {tables["split_join"]}
            UNION ALL
            SELECT * FROM {tables["soft"]}
        ) AS candidate_matches
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY query_id, candidate_id, query_token_position
            ORDER BY mechanism_precedence, match_strength DESC,
                candidate_token, candidate_token_position
        ) = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["query_statistics"]} AS
        WITH query_candidate_strength AS (
            SELECT
                scope.ukam_address_id_r AS query_id,
                scope.ukam_address_id_l AS candidate_id,
                query.query_token,
                query.query_token_position,
                COALESCE(matches.match_strength, 0.0) AS candidate_strength,
                scope.is_contextually_eligible
            FROM {tables["candidate_scope"]} AS scope
            JOIN {tables["query_tokens"]} AS query
                ON query.query_id = scope.ukam_address_id_r
            LEFT JOIN {tables["matches"]} AS matches
                ON matches.query_id = scope.ukam_address_id_r
               AND matches.candidate_id = scope.ukam_address_id_l
               AND matches.query_token_position = query.query_token_position
        ),
        counts AS (
            SELECT
                query_id,
                query_token,
                query_token_position,
                MAX(candidate_strength) FILTER (
                    WHERE is_contextually_eligible
                ) AS addressability_strength,
                COUNT(*) FILTER (
                    WHERE is_contextually_eligible AND candidate_strength > 0
                ) AS local_candidate_df
            FROM query_candidate_strength
            GROUP BY query_id, query_token, query_token_position
        ),
        with_group_size AS (
            SELECT
                counts.*,
                MAX(scope.eligible_candidate_count) AS eligible_candidate_count
            FROM counts
            JOIN {tables["candidate_scope"]} AS scope
                ON scope.ukam_address_id_r = counts.query_id
            GROUP BY ALL
        )
        SELECT
            *,
            addressability_strength = 0 AS is_unaddressed,
            local_candidate_df::DOUBLE / NULLIF(eligible_candidate_count, 0)
                >= {consensus_threshold} AS is_consensus,
            CASE
                WHEN addressability_strength = 0 THEN 0.0
                WHEN local_candidate_df::DOUBLE / NULLIF(eligible_candidate_count, 0)
                    >= {consensus_threshold} THEN 0.0
                ELSE ln(
                    (eligible_candidate_count + {local_frequency_smoothing})
                    / (local_candidate_df + {local_frequency_smoothing})
                )
            END AS local_query_weight
        FROM with_group_size
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["candidate_statistics"]} AS
        WITH candidate_document_frequency AS (
            SELECT
                candidate.query_id,
                candidate.candidate_token,
                COUNT(*) FILTER (WHERE scope.is_contextually_eligible)
                    AS local_candidate_df,
                MAX(scope.eligible_candidate_count) AS eligible_candidate_count
            FROM {tables["candidate_tokens"]} AS candidate
            JOIN {tables["candidate_scope"]} AS scope
                ON scope.ukam_address_id_r = candidate.query_id
               AND scope.ukam_address_id_l = candidate.candidate_id
            GROUP BY candidate.query_id, candidate.candidate_token
        )
        SELECT
            *,
            CASE
                WHEN local_candidate_df::DOUBLE / NULLIF(eligible_candidate_count, 0)
                    >= {consensus_threshold} THEN 0.0
                ELSE ln(
                    (eligible_candidate_count + {local_frequency_smoothing})
                    / (local_candidate_df + {local_frequency_smoothing})
                )
            END AS candidate_residual_weight
        FROM candidate_document_frequency
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["pair_strength"]} AS
        SELECT
            scope.ukam_address_id_r AS query_id,
            scope.ukam_address_id_l AS candidate_id,
            query.query_token,
            query.query_token_position,
            query.local_query_weight,
            query.addressability_strength,
            query.is_unaddressed,
            query.is_consensus,
            scope.is_contextually_eligible,
            COALESCE(matches.match_strength, 0.0) AS candidate_strength,
            matches.match_mechanism
        FROM {tables["candidate_scope"]} AS scope
        JOIN {tables["query_statistics"]} AS query
            ON query.query_id = scope.ukam_address_id_r
        LEFT JOIN {tables["matches"]} AS matches
            ON matches.query_id = scope.ukam_address_id_r
           AND matches.candidate_id = scope.ukam_address_id_l
           AND matches.query_token_position = query.query_token_position
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["pair_rival"]} AS
        WITH eligible_strengths AS (
            SELECT
                query_id,
                query_token_position,
                candidate_id,
                candidate_strength,
                DENSE_RANK() OVER (
                    PARTITION BY query_id, query_token_position
                    ORDER BY candidate_strength DESC
                ) AS strength_rank
            FROM {tables["pair_strength"]}
            WHERE is_contextually_eligible
        ),
        top_two AS (
            SELECT
                query_id,
                query_token_position,
                MAX(candidate_strength) FILTER (WHERE strength_rank = 1)
                    AS highest_strength,
                COUNT(*) FILTER (WHERE strength_rank = 1)
                    AS highest_strength_count,
                MAX(candidate_strength) FILTER (WHERE strength_rank = 2)
                    AS second_highest_strength
            FROM eligible_strengths
            GROUP BY query_id, query_token_position
        )
        SELECT
            current.*,
            CASE
                WHEN current.is_contextually_eligible
                    AND current.candidate_strength = top_two.highest_strength
                    AND top_two.highest_strength_count = 1
                    THEN COALESCE(top_two.second_highest_strength, 0.0)
                ELSE COALESCE(top_two.highest_strength, 0.0)
            END AS rival_strength
        FROM {tables["pair_strength"]} AS current
        LEFT JOIN top_two
            ON top_two.query_id = current.query_id
           AND top_two.query_token_position = current.query_token_position
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["candidate_support"]} AS
        WITH token_support AS (
            SELECT
                scope.ukam_address_id_r AS query_id,
                scope.ukam_address_id_l AS candidate_id,
                candidate.candidate_token,
                statistics.candidate_residual_weight,
                MAX(matches.match_strength) AS best_query_match_strength
            FROM {tables["candidate_scope"]} AS scope
            JOIN {tables["candidate_tokens"]} AS candidate
                ON candidate.query_id = scope.ukam_address_id_r
               AND candidate.candidate_id = scope.ukam_address_id_l
            JOIN {tables["candidate_statistics"]} AS statistics
                ON statistics.query_id = candidate.query_id
               AND statistics.candidate_token = candidate.candidate_token
            LEFT JOIN {tables["matches"]} AS matches
                ON matches.query_id = candidate.query_id
               AND matches.candidate_id = candidate.candidate_id
               AND matches.candidate_token = candidate.candidate_token
            WHERE scope.is_contextually_eligible
            GROUP BY ALL
        )
        SELECT
            query_id,
            candidate_id,
            COALESCE(
                SUM(candidate_residual_weight * COALESCE(best_query_match_strength, 0.0))
                / NULLIF(SUM(candidate_residual_weight), 0.0),
                0.0
            ) AS candidate_residual_support
        FROM token_support
        GROUP BY query_id, candidate_id
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["features"]} AS
        SELECT
            scope.ukam_address_id_r AS query_id,
            scope.ukam_address_id_l AS candidate_id,
            COALESCE(support.candidate_residual_support, 0.0)
                AS candidate_residual_support,
            COALESCE(
                SUM(pair.local_query_weight * greatest(
                    pair.rival_strength - pair.candidate_strength, 0.0
                ))
                / NULLIF(
                    SUM(pair.local_query_weight * pair.addressability_strength), 0.0
                ),
                0.0
            ) AS rival_explained_omission,
            COALESCE(
                SUM(pair.local_query_weight * (
                    pair.candidate_strength - pair.rival_strength
                ))
                / NULLIF(
                    SUM(pair.local_query_weight * pair.addressability_strength), 0.0
                ),
                0.0
            ) AS contrast_rate,
            COALESCE(
                SUM(pair.local_query_weight * pair.candidate_strength) FILTER (
                    WHERE pair.match_mechanism = 'split_join'
                )
                / NULLIF(
                    SUM(pair.local_query_weight * pair.addressability_strength), 0.0
                ),
                0.0
            ) AS split_join_rate,
            COALESCE(
                SUM(pair.local_query_weight * pair.candidate_strength) FILTER (
                    WHERE pair.match_mechanism = 'soft'
                )
                / NULLIF(
                    SUM(pair.local_query_weight * pair.addressability_strength), 0.0
                ),
                0.0
            ) AS soft_match_rate,
            COALESCE(
                SUM(pair.local_query_weight * pair.addressability_strength), 0.0
            ) AS addressable_distinguishing_weight
        FROM {tables["candidate_scope"]} AS scope
        LEFT JOIN {tables["pair_rival"]} AS pair
            ON pair.query_id = scope.ukam_address_id_r
           AND pair.candidate_id = scope.ukam_address_id_l
        LEFT JOIN {tables["candidate_support"]} AS support
            ON support.query_id = scope.ukam_address_id_r
           AND support.candidate_id = scope.ukam_address_id_l
        GROUP BY ALL
    """)
    con.execute(f"""
        CREATE TEMP TABLE {tables["scored"]} AS
        WITH raw_scores AS (
            SELECT
                scope.ukam_address_id_r AS query_id,
                scope.ukam_address_id_l AS candidate_id,
                scope.phase1_score,
                    scope.{contextual_base_score} AS contextual_base_score,
                scope.is_contextually_eligible,
                scope.eligible_candidate_count,
                features.* EXCLUDE (query_id, candidate_id),
                least(
                    1.0,
                    features.addressable_distinguishing_weight
                    / {evidence_sufficiency_target}
                ) AS evidence_scale,
                {candidate_support_weight} * features.candidate_residual_support
                    - {rival_omission_weight} * features.rival_explained_omission
                    + {contrast_weight} * features.contrast_rate
                    + {split_join_weight} * features.split_join_rate
                    + {soft_match_weight} * features.soft_match_rate
                    AS raw_contextual_score
            FROM {tables["candidate_scope"]} AS scope
            JOIN {tables["features"]} AS features
                ON features.query_id = scope.ukam_address_id_r
               AND features.candidate_id = scope.ukam_address_id_l
        ),
        centred AS (
            SELECT
                *,
                AVG(raw_contextual_score) FILTER (WHERE is_contextually_eligible)
                    OVER (PARTITION BY query_id) AS eligible_group_mean
            FROM raw_scores
        ),
        reference_winner AS (
            SELECT
                *,
                FIRST_VALUE(candidate_id) OVER (
                    PARTITION BY query_id
                    ORDER BY contextual_base_score DESC, candidate_id
                ) AS reference_winner_candidate_id,
                FIRST_VALUE(candidate_residual_support) OVER (
                    PARTITION BY query_id
                    ORDER BY contextual_base_score DESC, candidate_id
                ) AS reference_winner_residual_support
            FROM centred
        )
        SELECT
            *,
            CASE
                WHEN eligible_candidate_count < 2 THEN 0.0
                ELSE raw_contextual_score - eligible_group_mean
            END AS centred_contextual_score,
            CASE
                WHEN eligible_candidate_count < 2 THEN 0.0
                WHEN is_contextually_eligible THEN
                    CASE
                        WHEN (
                            raw_contextual_score - eligible_group_mean
                        ) > 0.0
                        AND NOT ({minimum_support_advantage_condition}) THEN 0.0
                        WHEN (
                            raw_contextual_score - eligible_group_mean
                        ) > 0.0
                        AND NOT ({minimum_candidate_support_condition}) THEN 0.0
                        ELSE {contextual_multiplier}
                            * (raw_contextual_score - eligible_group_mean)
                            * evidence_scale
                    END
                ELSE least(
                    0.0,
                    {contextual_multiplier}
                    * (raw_contextual_score - eligible_group_mean)
                    * evidence_scale
                )
            END AS gated_contextual_adjustment
        FROM reference_winner AS scored
    """)
    diagnostic_projection = ""
    diagnostic_joins = ""
    if config.include_diagnostics:
        con.execute(f"""
                CREATE TEMP TABLE {tables["diagnostics"]} AS
                SELECT
                    pair.query_id,
                    pair.candidate_id,
                    list(pair.query_token ORDER BY pair.query_token_position) FILTER (
                        WHERE pair.is_consensus
                    ) AS consensus_query_tokens,
                    list(pair.query_token ORDER BY pair.query_token_position) FILTER (
                        WHERE pair.is_unaddressed
                    ) AS unaddressed_query_tokens,
                    list(pair.query_token ORDER BY pair.query_token_position) FILTER (
                        WHERE pair.local_query_weight > 0
                            AND pair.candidate_strength > 0
                    ) AS matched_residual_tokens,
                    list(pair.query_token ORDER BY pair.query_token_position) FILTER (
                        WHERE pair.local_query_weight > 0
                          AND pair.rival_strength > pair.candidate_strength
                    ) AS rival_explained_tokens,
                    list(pair.query_token ORDER BY pair.query_token_position) FILTER (
                        WHERE pair.match_mechanism = 'split_join'
                    ) AS split_join_matches,
                    list(pair.query_token ORDER BY pair.query_token_position) FILTER (
                        WHERE pair.match_mechanism = 'soft'
                    ) AS soft_matches
                FROM {tables["pair_rival"]} AS pair
                GROUP BY pair.query_id, pair.candidate_id
            """)
        diagnostic_projection = """,
                scope.is_contextually_eligible,
                scope.eligible_candidate_count,
                scope.best_splink_match_weight,
                scope.splink_gap_from_best,
                scored.candidate_residual_support,
                scored.rival_explained_omission,
                scored.contrast_rate,
                scored.split_join_rate,
                scored.soft_match_rate,
                scored.addressable_distinguishing_weight,
                scored.evidence_scale,
                diagnostics.consensus_query_tokens,
                diagnostics.unaddressed_query_tokens,
                diagnostics.matched_residual_tokens,
                diagnostics.rival_explained_tokens,
                candidate_tokens.candidate_residual_tokens,
                diagnostics.split_join_matches,
                diagnostics.soft_matches"""
        diagnostic_joins = f"""
            LEFT JOIN {tables["diagnostics"]} AS diagnostics
                ON diagnostics.query_id = scope.ukam_address_id_r
               AND diagnostics.candidate_id = scope.ukam_address_id_l
            LEFT JOIN (
                SELECT
                    candidate.query_id,
                    candidate.candidate_id,
                    list(
                        candidate.candidate_token
                        ORDER BY candidate.candidate_token_position
                    ) FILTER (WHERE statistics.candidate_residual_weight > 0)
                        AS candidate_residual_tokens
                FROM {tables["candidate_tokens"]} AS candidate
                JOIN {tables["candidate_statistics"]} AS statistics
                    ON statistics.query_id = candidate.query_id
                   AND statistics.candidate_token = candidate.candidate_token
                GROUP BY candidate.query_id, candidate.candidate_id
            ) AS candidate_tokens
                ON candidate_tokens.query_id = scope.ukam_address_id_r
               AND candidate_tokens.candidate_id = scope.ukam_address_id_l"""

    output_columns = [column for column in df_predict.columns if column != "match_weight"]
    output_projection = ",\n            ".join(
        f"scope.{column}" for column in output_columns
    )
    con.execute(f"""
        CREATE TEMP TABLE {tables["final"]} AS
        SELECT
            {output_projection},
            greatest(
                -{contextual_cap},
                least({contextual_cap}, scored.gated_contextual_adjustment)
            ) AS contextual_adjustment,
            scope.{contextual_base_score} + greatest(
                -{contextual_cap},
                least({contextual_cap}, scored.gated_contextual_adjustment)
            ) AS contextual_match_weight,
            scope.{contextual_base_score} + greatest(
                -{contextual_cap},
                least({contextual_cap}, scored.gated_contextual_adjustment)
            ) AS match_weight
            {diagnostic_projection}
        FROM {tables["candidate_scope"]} AS scope
        JOIN {tables["scored"]} AS scored
            ON scored.query_id = scope.ukam_address_id_r
           AND scored.candidate_id = scope.ukam_address_id_l
        {diagnostic_joins}
    """)

    if telemetry is not None:
        telemetry["contextual_reranker_seconds"] = perf_counter() - started_at
        telemetry["soft_residual_pairs_considered"] = con.sql(
            f"SELECT count(*) FROM {tables['soft_candidates']}"
        ).fetchone()[0]
        telemetry["soft_jaro_winkler_pairs_evaluated"] = con.sql(
            f"SELECT count(*) FROM {tables['soft_scored']}"
        ).fetchone()[0]
        telemetry["soft_threshold_pairs"] = con.sql(
            f"SELECT count(*) FROM {tables['soft_qualified']}"
        ).fetchone()[0]
    for table_key, table_name in tables.items():
        if table_key != "final":
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
    return con.table(tables["final"])
