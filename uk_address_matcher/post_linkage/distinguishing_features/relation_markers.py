from __future__ import annotations

from duckdb import DuckDBPyConnection, DuckDBPyRelation

_RELATION_MARKERS_SQL = (
    "ADJACENT TO|OPPOSITE TO|REAR OFF|REAR OF|ADJ TO|NEXT TO|R O|RO|"
    "ADJACENT|OPPOSITE|BEHIND|REAR|ADJ|OPP"
)
_RELATION_STOP_TOKENS_SQL = (
    "('AND', 'AT', 'FOR', 'IN', 'MID', 'GLAM', 'GLAMORGAN', 'NR', 'OF', 'ON', "
    "'THE', 'TO')"
)
_WEAK_RELATION_TARGETS_SQL = (
    "('LAND', 'PLOT', 'SITE', 'GARAGE', 'YARD', 'FIELD', 'BUILDING', 'FLAT', "
    "'BUNGALOW', 'MOBILE HOME', 'CARAVAN', 'NEW DWELLING')"
)
_WEAK_RELATION_TARGET_PREFIXES_SQL = (
    "LAND|PLOT|SITE|GARAGE|YARD|FIELD|BUILDING|FLAT|BUNGALOW|"
    "MOBILE HOME|CARAVAN|NEW DWELLING"
)

TARGET_PHRASE_BOOST = 12.0
TARGET_ALL_TOKENS_BOOST = 8.0
ANCHOR_ONLY_WITH_TARGET_RIVAL_PENALTY = -10.0
ANCHOR_NUMBER_NO_TARGET_PENALTY = -6.0
ANCHOR_STREET_NO_TARGET_PENALTY = -4.0


def improve_predictions_using_relation_markers(
    *,
    df_predict: DuckDBPyRelation,
    con: DuckDBPyConnection,
) -> DuckDBPyRelation:
    """Prefer target-property evidence over relation-marker anchor evidence."""

    return con.sql(f"""
        WITH normalised AS (
            SELECT
                *,
                trim(
                    regexp_replace(
                        upper(clean_full_address_r), '[^A-Z0-9]+', ' ', 'g'
                    )
                )
                    AS source_address,
                trim(
                    regexp_replace(
                        upper(clean_full_address_l), '[^A-Z0-9]+', ' ', 'g'
                    )
                )
                    AS candidate_address
            FROM df_predict
        ),
        relation_parts AS (
            SELECT
                *,
                regexp_extract(
                    source_address,
                    '(?:^| )({_RELATION_MARKERS_SQL})(?: |$)',
                    1
                ) AS relation_marker
            FROM normalised
        ),
        split_addresses AS (
            SELECT
                *,
                trim(split_part(source_address, relation_marker, 1)) AS target_address,
                trim(split_part(source_address, relation_marker, 2)) AS anchor_address
            FROM relation_parts
            WHERE relation_marker != ''
        ),
        tokenised AS (
            SELECT
                *,
                list_filter(
                    regexp_split_to_array(target_address, ' '),
                    token -> len(token) > 1
                        AND token NOT IN {_RELATION_STOP_TOKENS_SQL}
                ) AS target_tokens,
                regexp_split_to_array(candidate_address, ' ') AS candidate_tokens,
                regexp_split_to_array(anchor_address, ' ') AS anchor_tokens,
                list_filter(
                    regexp_split_to_array(anchor_address, ' '),
                    token -> regexp_full_match(token, '[0-9]+')
                ) AS anchor_number_tokens
            FROM split_addresses
        ),
        features AS (
            SELECT
                *,
                CASE
                    WHEN target_address != ''
                        AND contains(
                            concat(' ', candidate_address, ' '),
                            concat(' ', target_address, ' ')
                        )
                    THEN TRUE
                    ELSE FALSE
                END AS target_phrase_match,
                CASE
                    WHEN len(target_tokens) > 0
                        AND list_has_all(candidate_tokens, target_tokens)
                    THEN TRUE
                    ELSE FALSE
                END AS target_all_tokens_match,
                len(list_intersect(candidate_tokens, target_tokens))
                    AS target_token_overlap_count,
                list_has_any(candidate_tokens, anchor_number_tokens)
                    AS anchor_number_match,
                len(list_intersect(candidate_tokens, anchor_tokens))
                    AS anchor_token_overlap_count,
                (
                    target_address IN {_WEAK_RELATION_TARGETS_SQL}
                    OR regexp_matches(
                        target_address,
                        '^({_WEAK_RELATION_TARGET_PREFIXES_SQL}) '
                    )
                ) AS weak_target
            FROM tokenised
        ),
        classified AS (
            SELECT
                *,
                (target_phrase_match OR target_all_tokens_match)
                    AS candidate_preserves_target,
                (
                    target_token_overlap_count = 0
                    AND (anchor_number_match OR anchor_token_overlap_count >= 3)
                ) AS candidate_is_anchor_only
            FROM features
        ),
        scored AS (
            SELECT
                *,
                bool_or(candidate_preserves_target) OVER (
                    PARTITION BY unique_id_r
                ) AS target_rival_exists
            FROM classified
        ),
        adjusted AS (
            SELECT
                *,
                CASE
                    WHEN weak_target THEN 0.0
                    ELSE
                        CASE
                            WHEN target_phrase_match THEN {TARGET_PHRASE_BOOST}
                            ELSE 0.0
                        END
                        + CASE
                            WHEN target_all_tokens_match THEN {TARGET_ALL_TOKENS_BOOST}
                            ELSE 0.0
                        END
                        + CASE
                            WHEN candidate_is_anchor_only AND target_rival_exists
                            THEN {ANCHOR_ONLY_WITH_TARGET_RIVAL_PENALTY}
                            ELSE 0.0
                          END
                        + CASE
                            WHEN anchor_number_match AND target_token_overlap_count = 0
                            THEN {ANCHOR_NUMBER_NO_TARGET_PENALTY}
                            ELSE 0.0
                          END
                        + CASE
                            WHEN anchor_token_overlap_count >= 3
                                AND target_token_overlap_count = 0
                            THEN {ANCHOR_STREET_NO_TARGET_PENALTY}
                            ELSE 0.0
                          END
                END AS relation_marker_adjustment
            FROM scored
        )
        SELECT
            adjusted.* EXCLUDE (
                source_address,
                candidate_address,
                relation_marker,
                target_address,
                anchor_address,
                target_tokens,
                candidate_tokens,
                anchor_tokens,
                anchor_number_tokens,
                target_phrase_match,
                target_all_tokens_match,
                target_token_overlap_count,
                anchor_number_match,
                anchor_token_overlap_count,
                weak_target,
                candidate_preserves_target,
                candidate_is_anchor_only,
                target_rival_exists,
                relation_marker_adjustment,
                match_weight
            ),
            match_weight + relation_marker_adjustment AS match_weight
        FROM adjusted

        UNION ALL

        SELECT
            normalised.* EXCLUDE (source_address, candidate_address, match_weight),
            match_weight
        FROM normalised
        WHERE regexp_extract(
            source_address,
            '(?:^| )({_RELATION_MARKERS_SQL})(?: |$)',
            1
        ) = ''
    """)
