from __future__ import annotations

from typing import TYPE_CHECKING

from uk_address_matcher.cleaning.steps.token_parsing import _derive_local_key_tokens
from uk_address_matcher.sql_pipeline.helpers import _uid
from uk_address_matcher.sql_pipeline.runner import create_sql_pipeline
from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage

if TYPE_CHECKING:
    import duckdb


def _canonical_local_key_gate(prefix: str) -> str:
    return (
        f"{prefix}pos <= 3 AND {prefix}n_uprns >= 2 AND "
        f"{prefix}df_uprns <= 3 AND "
        f"{prefix}df_uprns::DOUBLE / {prefix}n_uprns <= 0.5"
    )


@pipeline_stage(name="canonical_local_key_statistics", materialized=True)
def _canonical_local_key_statistics():
    return [
        CTEStep(
            "background",
            """
            SELECT postcode, count(DISTINCT unique_id) AS n_uprns
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
            "indexed_aliases",
            f"""
            SELECT ukam_address_id, list_sort(list(struct_pack(
                kind := kind, key := key, pos := pos,
                df_uprns := df_uprns::UTINYINT, n_uprns := n_uprns::UINTEGER
            )))::STRUCT(pos INTEGER, kind VARCHAR, key VARCHAR,
                df_uprns UTINYINT, n_uprns UINTEGER)[]
                AS local_key_index
            FROM {{occurrences}} occurrences
            JOIN {{frequencies}} frequencies USING (postcode, kind, key)
            JOIN {{background}} background USING (postcode)
            WHERE {_canonical_local_key_gate("")}
            GROUP BY ukam_address_id
        """,
        ),
        CTEStep(
            "final",
            """
            SELECT input.* EXCLUDE (local_key_tokens),
                coalesce(indexed.local_key_index, []::STRUCT(
                    pos INTEGER, kind VARCHAR, key VARCHAR,
                    df_uprns UTINYINT, n_uprns UINTEGER
                )[]) AS local_key_index
            FROM {input} input
            LEFT JOIN {indexed_aliases} indexed USING (ukam_address_id)
        """,
        ),
    ]


def prepare_source_local_keys(
    con: duckdb.DuckDBPyConnection,
    source: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    table_name = f"__ukam_source_local_keys_{_uid()}"
    input_name = f"{table_name}_input"
    con.register(input_name, source)
    existing = [
        column
        for column in ("local_key_tokens", "local_key_index")
        if column in source.columns
    ]
    exclude = f" EXCLUDE ({', '.join(existing)})" if existing else ""
    con.execute(f"""
        CREATE TEMP VIEW "{table_name}_base" AS
        SELECT *{exclude} FROM "{input_name}"
    """)
    prepared = create_sql_pipeline(
        con,
        input_rel=con.table(f"{table_name}_base"),
        stage_specs=[_derive_local_key_tokens()],
        pipeline_name="Prepare numericless source keys",
    ).run()
    con.execute(f'CREATE TEMP VIEW "{table_name}" AS {prepared.sql_query()}')
    return con.table(table_name)


def prepare_canonical_local_keys(
    con: duckdb.DuckDBPyConnection,
    canonical: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    table_name = f"__ukam_canonical_local_keys_{_uid()}"
    input_name = f"{table_name}_input"
    con.register(input_name, canonical)
    existing = [
        column
        for column in ("local_key_tokens", "local_key_index")
        if column in canonical.columns
    ]
    exclude = f" EXCLUDE ({', '.join(existing)})" if existing else ""
    con.execute(f"""
        CREATE TEMP VIEW "{table_name}_base" AS
        SELECT ukam_address_id, unique_id, postcode, clean_full_address
        FROM "{input_name}"
    """)
    prepared = create_sql_pipeline(
        con,
        input_rel=con.table(f"{table_name}_base"),
        stage_specs=[
            _derive_local_key_tokens(numericless_only=False),
            _canonical_local_key_statistics,
        ],
        pipeline_name="Prepare canonical postcode-wide keys",
    ).run()
    con.execute(f'CREATE TEMP TABLE "{table_name}_index" AS {prepared.sql_query()}')
    con.execute(f"""
        CREATE TEMP VIEW "{table_name}" AS
        SELECT canonical.*{exclude}, features.local_key_index
        FROM "{input_name}" canonical
        LEFT JOIN "{table_name}_index" features USING (ukam_address_id)
    """)
    return con.table(table_name)


def add_local_key_features(
    con: duckdb.DuckDBPyConnection,
    source: duckdb.DuckDBPyRelation,
    canonical: duckdb.DuckDBPyRelation,
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyRelation]:
    uid = _uid()
    raw_source = f"__ukam__name_raw_source_{uid}"
    canonical_input = f"__ukam__name_canonical_{uid}"
    source_input = f"__ukam__name_source_{uid}"
    feature_table = f"__ukam__name_source_features_{uid}"
    source_table = f"__ukam__name_featured_source_{uid}"
    canonical_table = f"__ukam__name_featured_canonical_{uid}"
    source_keys_table = f"__ukam__name_source_keys_{uid}"
    spacing_table = f"__ukam__name_spacing_candidates_{uid}"
    defaults = [
        f", NULL::{data_type} AS {column}"
        for column, data_type in (
            ("postcode", "VARCHAR"),
            ("resolved_canonical_id", "VARCHAR"),
            ("has_flat_indicator", "BOOLEAN"),
            ("has_business_unit", "BOOLEAN"),
        )
        if column not in source.columns
    ]
    con.register(raw_source, source)
    con.execute(f"""
        CREATE TEMP VIEW "{source_input}" AS
        SELECT * {"".join(defaults)} FROM "{raw_source}"
    """)
    source = prepare_source_local_keys(con, con.table(source_input))
    gate = """
        clean_full_address IS NOT NULL
        AND NOT regexp_matches(clean_full_address, '[0-9]')
        AND NOT regexp_matches(
            clean_full_address, '(^| )(EXCLUDING|EXCEPT|EXCL)( |$)'
        )
        AND resolved_canonical_id IS NULL
        AND has_flat_indicator IS FALSE AND has_business_unit IS FALSE
    """
    con.execute(f"""
        CREATE TEMP TABLE "{feature_table}" (
            alias_id VARCHAR, lk_level_by_alias MAP(VARCHAR, INTEGER),
            lk_anchor_uprns VARCHAR[]
        )
    """)
    if (
        con.execute(
            f"SELECT 1 FROM ({source.sql_query()}) WHERE {gate} LIMIT 1"
        ).fetchone()
        is not None
    ):
        if "local_key_index" not in canonical.columns:
            canonical = prepare_canonical_local_keys(con, canonical)
        con.register(canonical_input, canonical)
        con.execute(f"""
            CREATE TEMP TABLE "{source_keys_table}" AS
            SELECT DISTINCT ukam_address_id::VARCHAR AS alias_id,
                postcode, occurrence.pos, occurrence.kind, occurrence.key,
                min(occurrence.pos) FILTER (WHERE occurrence.kind = 'word')
                    OVER (PARTITION BY source.ukam_address_id) AS first_word_position,
                regexp_extract(source.clean_full_address, '^[^ ]+( [^ ]+)?', 0)
                    AS leading_tokens
            FROM ({source.sql_query()}) source,
                UNNEST(local_key_tokens) entries(occurrence)
            WHERE {gate}
        """)
        con.execute(f"""
            CREATE TEMP TABLE "{canonical_input}_keys" AS
            SELECT ukam_address_id, unique_id, postcode, local_key_index,
                regexp_extract(clean_full_address, '^[^ ]+( [^ ]+)?', 0)
                    AS leading_tokens
            FROM "{canonical_input}"
            WHERE postcode IN (SELECT postcode FROM "{source_keys_table}")
                AND len(local_key_index) > 0
        """)
        con.execute(f"""
            CREATE TEMP TABLE "{spacing_table}" AS
            WITH spellings AS (
                SELECT alias_id, postcode, key AS compact_key,
                    'phrase' AS kind,
                    left(key, split_at::INTEGER) || ' ' ||
                        substr(key, split_at::INTEGER + 1) AS spelling
                FROM "{source_keys_table}",
                    UNNEST(range(2, length(key) - 1)) splits(split_at)
                WHERE kind = 'word' AND pos <= 3 AND length(key) >= 6
                UNION ALL
                SELECT alias_id, postcode, replace(key, ' ', '') AS compact_key,
                    'word' AS kind, replace(key, ' ', '') AS spelling
                FROM "{source_keys_table}"
                WHERE kind = 'phrase' AND pos <= 3
                    AND length(replace(key, ' ', '')) >= 6
            )
            SELECT DISTINCT source.alias_id AS query_alias_id,
                canonical.ukam_address_id::VARCHAR AS canonical_alias_id,
                canonical.unique_id::VARCHAR AS entity_id,
                canonical.postcode, source.compact_key
            FROM "{canonical_input}_keys" canonical,
                UNNEST(local_key_index) entries(occurrence)
            JOIN spellings source
                ON canonical.postcode = source.postcode
                AND occurrence.kind = source.kind AND occurrence.key = source.spelling
            WHERE {_canonical_local_key_gate("occurrence.")}
        """)
        spacing_source = con.sql(f"""
            SELECT unique_id, postcode, clean_full_address
            FROM "{canonical_input}"
            WHERE postcode IN (SELECT postcode FROM "{spacing_table}")
        """)
        spacing_tokens = create_sql_pipeline(
            con,
            input_rel=spacing_source,
            stage_specs=[_derive_local_key_tokens(numericless_only=False)],
            pipeline_name="Check reached messy spacing candidates",
        ).run()
        con.execute(f"""
            INSERT INTO "{feature_table}"
            WITH source_keys AS MATERIALIZED (
                SELECT * FROM "{source_keys_table}"
            ), canonical_keys AS MATERIALIZED (
                SELECT DISTINCT ukam_address_id::VARCHAR AS alias_id,
                    unique_id::VARCHAR AS entity_id, postcode, leading_tokens,
                    occurrence.*
                FROM "{canonical_input}_keys" canonical,
                    UNNEST(local_key_index) entries(occurrence)
                WHERE occurrence.pos <= 3
                    AND occurrence.key IN (SELECT key FROM source_keys)
            ), hits AS MATERIALIZED (
                SELECT DISTINCT source.alias_id AS query_alias_id,
                    canonical.alias_id AS canonical_alias_id,
                    canonical.entity_id AS uprn, source.pos AS source_position,
                    source.kind, canonical.df_uprns,
                    (source.pos <= 3 AND (
                        (source.pos = canonical.pos AND (
                            source.pos = 1
                            OR (source.pos = 2
                                AND split_part(source.leading_tokens, ' ', 1)
                                    = split_part(canonical.leading_tokens, ' ', 1))
                            OR (source.pos = 3
                                AND source.leading_tokens = canonical.leading_tokens)
                        ))
                        OR (source.pos <= source.first_word_position AND (
                            canonical.pos = 1 OR (canonical.pos = 2 AND (
                                split_part(canonical.leading_tokens, ' ', 1)
                                    IN ('THE', 'GROUND')
                                OR regexp_full_match(
                                    split_part(canonical.leading_tokens, ' ', 1),
                                    '[0-9]+[A-Z]?'
                                )
                            ))
                        ))
                        OR (source.kind = 'phrase' AND canonical.pos = 1
                            AND split_part(source.key, ' ', 1) NOT IN (
                                'THE', 'OF', 'AT', 'NEAR', 'VIA', 'FROM', 'TO',
                                'AND', 'WITH', 'HOUSE', 'COTTAGE', 'COTTAGES',
                                'FARM', 'FARMHOUSE', 'BUNGALOW', 'LODGE', 'FLOOR',
                                'ROAD', 'STREET', 'LANE', 'TERRACE', 'FLAT', 'UNIT'
                            ))
                    )) AS is_name_key
                FROM source_keys source JOIN canonical_keys canonical
                    ON source.postcode = canonical.postcode
                    AND source.kind = canonical.kind AND source.key = canonical.key
                WHERE canonical.n_uprns >= 2 AND canonical.df_uprns <= 3
                    AND canonical.df_uprns::DOUBLE / canonical.n_uprns <= 0.5
            ), literal_levels AS MATERIALIZED (
                SELECT query_alias_id, canonical_alias_id, max(CASE
                    WHEN NOT is_name_key THEN 1
                    WHEN kind = 'phrase' AND df_uprns = 1
                        AND source_position <= 3 AND is_name_key THEN 6
                    WHEN kind = 'word' AND df_uprns = 1
                        AND source_position <= 3 AND is_name_key THEN 5
                    WHEN kind = 'phrase' THEN 4 ELSE 3 END)::INTEGER AS evidence_level
                FROM hits GROUP BY query_alias_id, canonical_alias_id
            ), anchors AS MATERIALIZED (
                SELECT query_alias_id, list(DISTINCT uprn ORDER BY uprn) AS anchor_uprns
                FROM (
                    SELECT *, min(source_position) OVER (PARTITION BY query_alias_id)
                        AS first_position
                    FROM hits WHERE df_uprns = 1 AND source_position <= 3
                        AND is_name_key
                ) candidates
                WHERE source_position = first_position GROUP BY query_alias_id
            ), compact_candidates AS MATERIALIZED (
                SELECT DISTINCT postcode, compact_key FROM "{spacing_table}"
            ), candidate_spellings AS (
                SELECT postcode, compact_key, compact_key AS spelling
                FROM compact_candidates
                UNION ALL
                SELECT postcode, compact_key,
                    left(compact_key, split_at::INTEGER) || ' ' ||
                        substr(compact_key, split_at::INTEGER + 1) AS spelling
                FROM compact_candidates,
                    UNNEST(range(2, length(compact_key) - 1)) splits(split_at)
            ), canonical_occurrences AS (
                SELECT postcode, unique_id, occurrence.key
                FROM ({spacing_tokens.sql_query()}) canonical,
                    UNNEST(local_key_tokens) entries(occurrence)
            ), spacing_frequencies AS (
                SELECT candidates.postcode, candidates.compact_key,
                    count(DISTINCT canonical.unique_id) AS n_uprns
                FROM candidate_spellings candidates JOIN canonical_occurrences canonical
                    ON candidates.postcode = canonical.postcode
                    AND candidates.spelling = canonical.key
                GROUP BY candidates.postcode, candidates.compact_key
            ), spacing_levels AS (
                SELECT DISTINCT source.query_alias_id,
                    source.canonical_alias_id, 2 AS evidence_level
                FROM "{spacing_table}" source
                JOIN spacing_frequencies USING (postcode, compact_key)
                LEFT JOIN anchors ON source.query_alias_id = anchors.query_alias_id
                WHERE spacing_frequencies.n_uprns = 1
                    AND NOT (len(coalesce(anchors.anchor_uprns, []::VARCHAR[])) = 1
                        AND NOT list_contains(anchors.anchor_uprns, source.entity_id))
                    AND NOT EXISTS (
                        SELECT 1 FROM literal_levels literal
                        WHERE literal.query_alias_id = source.query_alias_id
                            AND literal.canonical_alias_id = source.canonical_alias_id
                    )
            ), levels AS (
                SELECT * FROM literal_levels UNION ALL SELECT * FROM spacing_levels
            )
            SELECT query_alias_id AS alias_id,
                map(list(canonical_alias_id ORDER BY canonical_alias_id),
                    list(evidence_level ORDER BY canonical_alias_id))
                    AS lk_level_by_alias,
                coalesce(first(anchors.anchor_uprns), []::VARCHAR[]) AS lk_anchor_uprns
            FROM levels LEFT JOIN anchors USING (query_alias_id)
            GROUP BY query_alias_id
        """)
        con.execute(f'DROP TABLE "{source_keys_table}"')
        con.execute(f'DROP TABLE "{spacing_table}"')
        con.execute(f'DROP TABLE "{canonical_input}_keys"')
    con.execute(f"""
        CREATE TEMP VIEW "{source_table}" AS
        SELECT source.*,
            coalesce(features.lk_level_by_alias, MAP([]::VARCHAR[], []::INTEGER[]))
                AS lk_level_by_alias,
            coalesce(features.lk_anchor_uprns, []::VARCHAR[]) AS lk_anchor_uprns,
            features.alias_id IS NOT NULL AS lk_eligible
        FROM ({source.sql_query()}) AS source
        LEFT JOIN "{feature_table}" AS features
            ON source.ukam_address_id::VARCHAR = features.alias_id
    """)
    con.register(canonical_input, canonical)
    con.execute(f"""
        CREATE TEMP VIEW "{canonical_table}" AS
        SELECT *, MAP([]::VARCHAR[], []::INTEGER[]) AS lk_level_by_alias,
            []::VARCHAR[] AS lk_anchor_uprns, false AS lk_eligible
        FROM "{canonical_input}"
    """)
    return con.table(source_table), con.table(canonical_table)
