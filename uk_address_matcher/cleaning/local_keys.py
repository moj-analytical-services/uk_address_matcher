from __future__ import annotations

from typing import TYPE_CHECKING

from uk_address_matcher.cleaning.steps.token_parsing import _derive_local_key_tokens
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import InputBinding, create_sql_pipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb

_LOCAL_KEY_CACHE_COLUMNS = (
    "local_key_tokens",
    "local_key_token_hash",
    "local_key_token_version",
    "local_key_numericless_only",
    "local_key_index",
    "local_key_background",
    "local_key_version",
)


def _canonical_local_key_gate(prefix: str) -> str:
    return (
        f"{prefix}pos <= 3 AND {prefix}n_uprns >= 2 AND ("
        f"({prefix}df_uprns <= 3 AND "
        f"{prefix}df_uprns::DOUBLE / {prefix}n_uprns <= 0.5) "
        f"OR ({prefix}spacing_df_uprns = 1 AND "
        f"length(replace({prefix}key, ' ', '')) >= 6))"
    )


@pipeline_stage(name="canonical_local_key_statistics", materialized=True)
def _canonical_local_key_statistics():
    return [
        CTEStep(
            "background",
            """
            SELECT postcode, count(*) AS aliases, count(DISTINCT unique_id) AS n_uprns,
                bit_xor(hash(ukam_address_id::VARCHAR, unique_id::VARCHAR,
                    postcode, clean_full_address)) AS signature,
                min(ukam_address_id::VARCHAR) AS metadata_alias
            FROM {input} GROUP BY postcode
            """,
        ),
        CTEStep(
            "occurrences",
            """
            SELECT input.ukam_address_id, input.unique_id, input.postcode,
                occurrence.pos, occurrence.kind, occurrence.key
            FROM {input} input, UNNEST(local_key_tokens) entries(occurrence)
        """,
        ),
        CTEStep(
            "frequencies",
            """
            SELECT postcode, kind, key, count(DISTINCT unique_id) AS df_uprns
            FROM {occurrences} GROUP BY postcode, kind, key
        """,
        ),
        CTEStep(
            "spacing_frequencies",
            """
            SELECT postcode, replace(key, ' ', '') AS compact_key,
                count(DISTINCT unique_id) AS spacing_df_uprns
            FROM {occurrences}
            WHERE length(replace(key, ' ', '')) >= 6
            GROUP BY postcode, compact_key
        """,
        ),
        CTEStep(
            "indexed_aliases",
            f"""
            SELECT ukam_address_id, list(struct_pack(
                pos := pos, kind := kind, key := key,
                df_uprns := df_uprns, n_uprns := n_uprns,
                spacing_df_uprns := spacing_df_uprns
            ) ORDER BY kind, key, pos) AS local_key_index
            FROM {{occurrences}} occurrences
            JOIN {{frequencies}} frequencies USING (postcode, kind, key)
            JOIN {{background}} background USING (postcode)
            LEFT JOIN {{spacing_frequencies}} spacing
                ON occurrences.postcode = spacing.postcode
                AND replace(occurrences.key, ' ', '') = spacing.compact_key
            WHERE {_canonical_local_key_gate("")}
            GROUP BY ukam_address_id
        """,
        ),
        CTEStep(
            "final",
            """
            SELECT input.* EXCLUDE (local_key_tokens), 4 AS local_key_version,
                CASE WHEN input.ukam_address_id::VARCHAR = background.metadata_alias
                THEN [struct_pack(aliases := background.aliases,
                    n_uprns := background.n_uprns,
                    signature := background.signature)] ELSE [] END
                    AS local_key_background,
                coalesce(indexed.local_key_index, []::STRUCT(
                    pos INTEGER, kind VARCHAR, key VARCHAR,
                    df_uprns BIGINT, n_uprns BIGINT, spacing_df_uprns BIGINT
                )[]) AS local_key_index
            FROM {input} input
            LEFT JOIN {background} background
                ON input.postcode IS NOT DISTINCT FROM background.postcode
            LEFT JOIN {indexed_aliases} indexed USING (ukam_address_id)
        """,
        ),
    ]


def prepare_source_local_keys(
    con: duckdb.DuckDBPyConnection,
    source: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    if set(_LOCAL_KEY_CACHE_COLUMNS[:4]).issubset(source.columns):
        stale = (
            source.filter(
                "local_key_token_version IS DISTINCT FROM 2 OR "
                "local_key_token_hash IS DISTINCT FROM "
                "hash(clean_full_address, len(numeric_tokens)) OR "
                "local_key_numericless_only IS DISTINCT FROM true"
            )
            .limit(1)
            .fetchone()
        )
        if stale is None:
            return source
    existing = [column for column in _LOCAL_KEY_CACHE_COLUMNS if column in source.columns]
    if existing:
        source = source.select(f"* EXCLUDE ({', '.join(existing)})")
    return create_sql_pipeline(
        con,
        input_rel=source,
        stage_specs=[_derive_local_key_tokens()],
        pipeline_name="Prepare numericless source keys",
    ).run()


def prepare_canonical_local_keys(
    con: duckdb.DuckDBPyConnection,
    canonical: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Reuse postcode statistics only while their canonical background matches."""
    if set(_LOCAL_KEY_CACHE_COLUMNS[1:]).issubset(canonical.columns) and (
        "spacing_df_uprns"
        in dict(
            canonical.types[canonical.columns.index("local_key_index")]
            .children[0][1]
            .children
        )
    ):
        sparse_background = (
            canonical.types[canonical.columns.index("local_key_background")].id == "list"
        )
        cache = (
            canonical
            if sparse_background
            else canonical.select(
                "* REPLACE ([local_key_background] AS local_key_background)"
            )
        )
        stale = (
            cache.query(
                "canonical",
                f"""
            WITH background AS (
                SELECT postcode, count(*) AS aliases,
                    count(DISTINCT unique_id) AS n_uprns,
                    bit_xor(hash(ukam_address_id::VARCHAR, unique_id::VARCHAR,
                        postcode, clean_full_address)) AS signature,
                    sum(len(local_key_background)) AS cache_entries
                FROM canonical GROUP BY postcode
            )
            SELECT 1 FROM canonical
            JOIN background ON canonical.postcode IS NOT DISTINCT FROM background.postcode
            WHERE local_key_version IS NULL
                OR local_key_version NOT IN ({"4" if sparse_background else "2, 3"})
                OR background.cache_entries IS DISTINCT FROM
                    {"1" if sparse_background else "background.aliases"}
                OR local_key_background IS NULL OR len(local_key_background) > 1
                OR local_key_token_version IS DISTINCT FROM 2
                OR local_key_numericless_only IS DISTINCT FROM false
                OR local_key_token_hash IS DISTINCT FROM hash(clean_full_address)
                OR (len(local_key_background) = 1 AND (
                    local_key_background[1].aliases IS DISTINCT FROM background.aliases
                    OR local_key_background[1].n_uprns IS DISTINCT FROM background.n_uprns
                    OR local_key_background[1].signature
                        IS DISTINCT FROM background.signature
                ))
        """,
            )
            .limit(1)
            .fetchone()
        )
        if stale is None:
            if not sparse_background:
                projection = "*"
                if "local_key_tokens" in canonical.columns:
                    projection += " EXCLUDE (local_key_tokens)"
                return canonical.select(
                    f"""
                    {projection} REPLACE (4 AS local_key_version,
                        CASE WHEN ukam_address_id::VARCHAR = min(ukam_address_id::VARCHAR)
                            OVER (PARTITION BY postcode)
                        THEN [local_key_background] ELSE [] END AS local_key_background,
                        list_filter(local_key_index, occurrence ->
                            ({_canonical_local_key_gate("occurrence.")}))
                            AS local_key_index)
                    """,
                )
            return canonical
    existing = [
        column for column in _LOCAL_KEY_CACHE_COLUMNS if column in canonical.columns
    ]
    if existing:
        canonical = canonical.select(f"* EXCLUDE ({', '.join(existing)})")
    return create_sql_pipeline(
        con,
        input_rel=canonical,
        stage_specs=[
            _derive_local_key_tokens(numericless_only=False),
            _canonical_local_key_statistics,
        ],
        pipeline_name="Prepare canonical postcode-wide keys",
    ).run()


def _local_key_source_gate() -> str:
    return (
        "clean_full_address IS NOT NULL "
        "AND NOT regexp_matches(clean_full_address, '[0-9]') "
        "AND postcode IS NOT NULL AND postcode <> '' "
        "AND resolved_canonical_id IS NULL "
        "AND has_flat_indicator IS FALSE AND has_business_unit IS FALSE"
    )


@pipeline_stage(name="local_key_features", materialized=True)
def _local_key_features() -> list[CTEStep]:
    gate = _local_key_source_gate()
    return [
        CTEStep(
            "lk_input",
            f"""
            SELECT ukam_address_id::VARCHAR AS alias_id, postcode, local_key_tokens
            FROM {{local_key_source}} WHERE {gate}
        """,
        ),
        CTEStep(
            "lk_source_keys",
            """
            SELECT DISTINCT alias_id, postcode,
                occurrence.pos, occurrence.kind, occurrence.key
            FROM {lk_input}, UNNEST(local_key_tokens) entries(occurrence)
        """,
        ),
        CTEStep(
            "lk_canonical_keys",
            """
            SELECT DISTINCT ukam_address_id::VARCHAR AS alias_id,
                unique_id::VARCHAR AS entity_id, postcode,
                occurrence.pos, occurrence.kind, occurrence.key,
                occurrence.df_uprns, occurrence.n_uprns, occurrence.spacing_df_uprns
            FROM {local_key_canonical}, UNNEST(local_key_index) entries(occurrence)
            WHERE occurrence.pos <= 3
                AND postcode IN (SELECT postcode FROM {lk_source_keys})
        """,
        ),
        CTEStep(
            "lk_hits",
            """
            SELECT DISTINCT s.alias_id AS query_alias_id,
                k.alias_id AS canonical_alias_id, k.entity_id AS uprn,
                s.pos AS source_position, k.pos AS canonical_position,
                s.kind, s.key, k.df_uprns, k.n_uprns
            FROM {lk_source_keys} s JOIN {lk_canonical_keys} k
                ON s.postcode = k.postcode AND s.kind = k.kind AND s.key = k.key
            WHERE k.n_uprns >= 2 AND k.df_uprns <= 3
                AND k.df_uprns::DOUBLE / k.n_uprns <= 0.5
        """,
        ),
        CTEStep(
            "lk_levels",
            """
            SELECT query_alias_id, canonical_alias_id, max(CASE
                WHEN kind = 'phrase' AND df_uprns = 1 AND source_position <= 3 THEN 6
                WHEN kind = 'word' AND df_uprns = 1 AND source_position <= 3 THEN 5
                WHEN kind = 'phrase' THEN 4
                WHEN kind = 'word' THEN 3 ELSE 0 END)::INTEGER AS evidence_level
            FROM {lk_hits} GROUP BY query_alias_id, canonical_alias_id
        """,
        ),
        CTEStep(
            "lk_anchors",
            """
            WITH first_anchor AS (
                SELECT query_alias_id, min(source_position) AS first_position
                FROM {lk_hits} WHERE df_uprns = 1 AND source_position <= 3
                GROUP BY query_alias_id
            )
            SELECT hits.query_alias_id, list(DISTINCT hits.uprn ORDER BY hits.uprn)
                AS anchor_uprns
            FROM {lk_hits} hits JOIN first_anchor anchor USING (query_alias_id)
            WHERE hits.source_position = anchor.first_position AND hits.df_uprns = 1
            GROUP BY hits.query_alias_id
        """,
        ),
        CTEStep(
            "lk_spacing_levels",
            """
            SELECT DISTINCT source.alias_id AS query_alias_id,
                canonical.alias_id AS canonical_alias_id, 2::INTEGER AS evidence_level
            FROM {lk_source_keys} source JOIN {lk_canonical_keys} canonical
                ON source.postcode = canonical.postcode
                AND replace(source.key, ' ', '') = replace(canonical.key, ' ', '')
                AND source.kind <> canonical.kind
            LEFT JOIN {lk_anchors} anchors ON source.alias_id = anchors.query_alias_id
            WHERE source.pos <= 3 AND canonical.spacing_df_uprns = 1
                AND canonical.n_uprns >= 2
                AND length(replace(canonical.key, ' ', '')) >= 6
                AND NOT (len(coalesce(anchors.anchor_uprns, []::VARCHAR[])) = 1
                    AND NOT list_contains(anchors.anchor_uprns, canonical.entity_id))
                AND NOT EXISTS (
                    SELECT 1 FROM {lk_levels} literal
                    WHERE literal.query_alias_id = source.alias_id
                        AND literal.canonical_alias_id = canonical.alias_id
                )
        """,
        ),
        CTEStep(
            "lk_combined_levels",
            """
            SELECT * FROM {lk_levels}
            UNION ALL SELECT * FROM {lk_spacing_levels}
        """,
        ),
        CTEStep(
            "lk_sources",
            """
            SELECT source.*,
                coalesce(features.levels, MAP([]::VARCHAR[], []::INTEGER[]))
                    AS lk_level_by_alias,
                coalesce(anchors.anchor_uprns, []::VARCHAR[]) AS lk_anchor_uprns,
                features.levels IS NOT NULL AS lk_eligible
            FROM {local_key_source} source
            LEFT JOIN (
                SELECT query_alias_id,
                    map(list(canonical_alias_id ORDER BY canonical_alias_id),
                        list(evidence_level ORDER BY canonical_alias_id)) AS levels
                FROM {lk_combined_levels} GROUP BY query_alias_id
            ) features ON source.ukam_address_id::VARCHAR = features.query_alias_id
            LEFT JOIN {lk_anchors} anchors
                ON source.ukam_address_id::VARCHAR = anchors.query_alias_id
        """,
        ),
    ]


def add_local_key_features(
    con: duckdb.DuckDBPyConnection,
    source: duckdb.DuckDBPyRelation,
    canonical: duckdb.DuckDBPyRelation,
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    for column, data_type in (
        ("resolved_canonical_id", "VARCHAR"),
        ("has_flat_indicator", "BOOLEAN"),
        ("has_business_unit", "BOOLEAN"),
    ):
        if column not in source.columns:
            source = source.select(f"*, NULL::{data_type} AS {column}")
    source = prepare_source_local_keys(con, source)
    neutral_features = """
        *,
        MAP([]::VARCHAR[], []::INTEGER[]) AS lk_level_by_alias,
        []::VARCHAR[] AS lk_anchor_uprns, false AS lk_eligible
    """
    if source.filter(_local_key_source_gate()).limit(1).fetchone() is None:
        return source.select(neutral_features), canonical.select(neutral_features)
    canonical = prepare_canonical_local_keys(con, canonical)
    pipeline = create_sql_pipeline(
        con,
        [
            InputBinding("local_key_source", source),
            InputBinding("local_key_canonical", canonical),
        ],
        [_local_key_features],
        pipeline_name="Postcode-distinguishing property names",
    )
    features = (
        pipeline.run()
        .filter("lk_eligible")
        .select(
            "ukam_address_id::VARCHAR AS alias_id, lk_level_by_alias, lk_anchor_uprns"
        )
    )
    feature_table = f"__ukam__name_source_features_{_uid()}"
    con.execute(f'CREATE TEMP TABLE "{feature_table}" AS {features.sql_query()}')
    featured_source = (
        source.set_alias("source")
        .join(
            con.table(feature_table).set_alias("features"),
            "source.ukam_address_id::VARCHAR = features.alias_id",
            how="left",
        )
        .select("""
        source.*,
            coalesce(features.lk_level_by_alias, MAP([]::VARCHAR[], []::INTEGER[]))
                AS lk_level_by_alias,
            coalesce(features.lk_anchor_uprns, []::VARCHAR[]) AS lk_anchor_uprns,
            features.alias_id IS NOT NULL AS lk_eligible
    """)
    )
    featured_canonical = canonical.select(neutral_features)
    return featured_source, featured_canonical
