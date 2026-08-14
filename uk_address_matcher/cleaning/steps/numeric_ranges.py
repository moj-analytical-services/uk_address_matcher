from __future__ import annotations

from uk_address_matcher.sql_pipeline.steps import pipeline_stage


@pipeline_stage(
    name="derive_numeric_range",
    description="Derive typed numeric-range and scalar metadata from numeric tokens",
    tags=["token_extraction"],
)
def _derive_numeric_range(
    maximum_width: int = 25,
) -> str:
    """Build typed range metadata from the existing numeric token list."""
    return f"""
    WITH classified_tokens AS (
        SELECT
            input.*,
            list_filter(
                regexp_extract_all(
                    clean_full_address,
                    '\\b\\d{{1,5}}[A-Z]?-\\d{{1,5}}[A-Z]?\\b'
                ),
                token -> TRUE
            ) AS range_tokens,
            CASE
                WHEN len(regexp_extract_all(
                    clean_full_address,
                    '\\b\\d{{1,5}}[A-Z]?-\\d{{1,5}}[A-Z]?\\b'
                )) = 0
                    THEN list_filter(
                        numeric_tokens,
                        token -> regexp_matches(token, '^\\d{{1,5}}[A-Z]?$')
                    )
                ELSE []::VARCHAR[]
            END AS scalar_tokens
        FROM {{input}} AS input
    )
    SELECT
        * EXCLUDE (range_tokens, scalar_tokens),
        len(range_tokens)::UTINYINT AS numeric_range_count,
        list_transform(
            range_tokens,
            token -> struct_pack(
                raw := token,
                lower := TRY_CAST(
                    regexp_extract(token, '^(\\d{{1,5}})', 1) AS UINTEGER
                ),
                upper := TRY_CAST(
                    regexp_extract(token, '-(\\d{{1,5}})', 1) AS UINTEGER
                ),
                width := GREATEST(
                    0,
                    TRY_CAST(regexp_extract(
                        token, '-(\\d{{1,5}})', 1
                    ) AS BIGINT)
                    - TRY_CAST(regexp_extract(
                        token, '^(\\d{{1,5}})', 1
                    ) AS BIGINT)
                )::UINTEGER,
                lower_suffix := NULLIF(
                    regexp_extract(token, '^(\\d{{1,5}})([A-Z])?-', 2),
                    ''
                ),
                upper_suffix := NULLIF(
                    regexp_extract(token, '-\\d{{1,5}}([A-Z])?$', 1),
                    ''
                ),
                role := CASE
                    WHEN regexp_matches(clean_full_address, '\\b(REF|REFERENCE)\\b')
                        THEN 3::UTINYINT
                    ELSE 1::UTINYINT
                END,
                flags := (
                    CASE
                        WHEN TRY_CAST(regexp_extract(
                            token, '^(\\d{{1,5}})', 1
                        ) AS UINTEGER)
                            > TRY_CAST(regexp_extract(
                                token, '-(\\d{{1,5}})', 1
                            ) AS UINTEGER)
                        THEN 1 ELSE 0
                    END
                    + CASE
                        WHEN TRY_CAST(regexp_extract(
                            token, '^(\\d{{1,5}})', 1
                        ) AS UINTEGER)
                            = TRY_CAST(regexp_extract(
                                token, '-(\\d{{1,5}})', 1
                            ) AS UINTEGER)
                        THEN 2 ELSE 0
                    END
                    + CASE
                        WHEN regexp_matches(token, '^\\d{{1,5}}[A-Z]-')
                            OR regexp_matches(token, '-\\d{{1,5}}[A-Z]$')
                        THEN 4 ELSE 0
                    END
                    + CASE
                        WHEN TRY_CAST(regexp_extract(
                            token, '-(\\d{{1,5}})', 1
                        ) AS BIGINT)
                            - TRY_CAST(regexp_extract(
                                token, '^(\\d{{1,5}})', 1
                            ) AS BIGINT) > {maximum_width}
                        THEN 8 ELSE 0
                    END
                    + CASE
                        WHEN regexp_matches(clean_full_address, '\\b(REF|REFERENCE)\\b')
                        THEN 16 ELSE 0
                    END
                )::UTINYINT,
                lower_tf := NULL::DOUBLE
            )
        ) AS numeric_range_attributes,
        list_transform(
            scalar_tokens,
            token -> TRY_CAST(regexp_extract(token, '^\\d{{1,5}}', 0) AS UINTEGER)
        ) AS numeric_scalar_tokens,
        list_transform(
            scalar_tokens,
            token -> NULLIF(regexp_extract(token, '[A-Z]$', 0), '')
        ) AS numeric_scalar_suffixes,
        list_transform(
            scalar_tokens,
            token -> 0::UTINYINT
        ) AS numeric_scalar_roles
    FROM classified_tokens
    """


@pipeline_stage(
    name="add_numeric_range_lower_endpoint_tf",
    description="Attach lower-endpoint TF to typed numeric-range attributes",
    tags=["term_frequency"],
)
def _add_numeric_range_lower_endpoint_tf() -> str:
    """Add lower-endpoint TF without carrying numeric TF columns downstream."""
    return """
    WITH tf_lookup AS (
        SELECT
            map_from_entries(list(struct_pack(
                key := CAST(numeric_token AS VARCHAR),
                value := tf_numeric_token
            ))) AS values
        FROM __ukam__tmp_numeric_term_frequencies
    )
    SELECT
        input.* EXCLUDE (numeric_range_attributes),
        list_transform(
            input.numeric_range_attributes,
            attribute -> struct_pack(
                raw := attribute.raw,
                lower := attribute.lower,
                upper := attribute.upper,
                width := attribute.width,
                lower_suffix := attribute.lower_suffix,
                upper_suffix := attribute.upper_suffix,
                role := attribute.role,
                flags := attribute.flags,
                lower_tf := lookup.values[
                    CAST(attribute.lower AS VARCHAR)
                ]
            )
        ) AS numeric_range_attributes
    FROM {input} AS input
    CROSS JOIN tf_lookup AS lookup
    """
