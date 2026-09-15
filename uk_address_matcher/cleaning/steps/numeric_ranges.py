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
                numeric_tokens,
                token -> regexp_matches(
                    token,
                    '^\\d{{1,5}}[A-Z]?-\\d{{1,5}}[A-Z]?$'
                )
            ) AS range_tokens
        FROM {{input}} AS input
    ),
    range_attributes AS (
        SELECT
            classified_tokens.*,
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
                            WHEN regexp_matches(
                                clean_full_address, '\\b(REF|REFERENCE)\\b'
                            )
                            THEN 16 ELSE 0
                        END
                    )::UTINYINT,
                    lower_tf := NULL::DOUBLE
                )
            ) AS parsed_range_attributes
        FROM classified_tokens
    )
    SELECT
        * EXCLUDE (range_tokens, parsed_range_attributes),
        CASE
            WHEN len(parsed_range_attributes) > 0
            THEN list_extract(parsed_range_attributes, 1)
            ELSE NULL
        END AS numeric_range
    FROM range_attributes
    """


@pipeline_stage(
    name="add_numeric_range_lower_endpoint_tf",
    description="Attach lower-endpoint TF to typed numeric-range attributes",
    tags=["term_frequency"],
)
def _add_numeric_range_lower_endpoint_tf() -> str:
    """Add lower-endpoint TF to the nullable numeric-range struct."""
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
        input.* EXCLUDE (numeric_range),
        CASE
            WHEN input.numeric_range IS NULL THEN NULL
            ELSE struct_pack(
                raw := input.numeric_range.raw,
                lower := input.numeric_range.lower,
                upper := input.numeric_range.upper,
                width := input.numeric_range.width,
                lower_suffix := input.numeric_range.lower_suffix,
                upper_suffix := input.numeric_range.upper_suffix,
                role := input.numeric_range.role,
                flags := input.numeric_range.flags,
                lower_tf := lookup.values[
                    CAST(input.numeric_range.lower AS VARCHAR)
                ]
            )
        END AS numeric_range
    FROM {input} AS input
    CROSS JOIN tf_lookup AS lookup
    """
