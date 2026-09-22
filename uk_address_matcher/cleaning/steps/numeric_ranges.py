from __future__ import annotations

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="derive_numeric_range",
    description="Derive typed numeric-range and scalar metadata from numeric tokens",
    tags=["token_extraction"],
)
def _derive_numeric_range(
    maximum_width: int = 25,
) -> list[CTEStep]:
    """Build typed range metadata from the existing numeric token list."""
    return [
        CTEStep(
            "classified_tokens",
            """
            SELECT
                input.*,
                list_filter(
                    numeric_tokens,
                    token -> regexp_matches(
                        token,
                        '^\\d{1,5}[A-Z]?-\\d{1,5}[A-Z]?$'
                    )
                ) AS range_tokens,
                regexp_matches(
                    clean_full_address,
                    '\\b(REF|REFERENCE)\\b'
                ) AS __has_reference
            FROM {input} AS input
            """,
        ),
        CTEStep(
            "range_endpoints",
            """
            SELECT
                classified_tokens.*,
                list_transform(
                    range_tokens,
                    token -> regexp_extract(
                        token,
                        '^(\\d{1,5})([A-Z]?)-(\\d{1,5})([A-Z]?)$',
                        ['lower', 'lower_suffix', 'upper', 'upper_suffix']
                    )
                ) AS parsed_range_endpoints
            FROM {classified_tokens} AS classified_tokens
            """,
        ),
        CTEStep(
            "range_attributes",
            f"""
            SELECT
                range_endpoints.*,
                list_transform(
                    parsed_range_endpoints,
                    endpoint -> struct_pack(
                        raw := endpoint.lower || endpoint.lower_suffix || '-'
                            || endpoint.upper || endpoint.upper_suffix,
                        lower := TRY_CAST(endpoint.lower AS UINTEGER),
                        upper := TRY_CAST(endpoint.upper AS UINTEGER),
                        width := GREATEST(
                            0,
                            TRY_CAST(endpoint.upper AS BIGINT)
                                - TRY_CAST(endpoint.lower AS BIGINT)
                        )::UINTEGER,
                        lower_suffix := NULLIF(endpoint.lower_suffix, ''),
                        upper_suffix := NULLIF(endpoint.upper_suffix, ''),
                        role := CASE
                            WHEN __has_reference THEN 3::UTINYINT
                            ELSE 1::UTINYINT
                        END,
                        flags := (
                            CASE
                                WHEN TRY_CAST(endpoint.lower AS UINTEGER)
                                    > TRY_CAST(endpoint.upper AS UINTEGER)
                                THEN 1 ELSE 0
                            END
                            + CASE
                                WHEN TRY_CAST(endpoint.lower AS UINTEGER)
                                    = TRY_CAST(endpoint.upper AS UINTEGER)
                                THEN 2 ELSE 0
                            END
                            + CASE
                                WHEN endpoint.lower_suffix != ''
                                    OR endpoint.upper_suffix != ''
                                THEN 4 ELSE 0
                            END
                            + CASE
                                WHEN TRY_CAST(endpoint.upper AS BIGINT)
                                    - TRY_CAST(endpoint.lower AS BIGINT)
                                    > {maximum_width}
                                THEN 8 ELSE 0
                            END
                            + CASE WHEN __has_reference THEN 16 ELSE 0 END
                        )::UTINYINT,
                        lower_tf := NULL::DOUBLE
                    )
                ) AS parsed_range_attributes
            FROM {{range_endpoints}} AS range_endpoints
            """,
        ),
        CTEStep(
            "final",
            """
            SELECT
                * EXCLUDE (
                    range_tokens,
                    __has_reference,
                    parsed_range_endpoints,
                    parsed_range_attributes
                ),
                CASE
                    WHEN len(parsed_range_attributes) > 0
                    THEN list_extract(parsed_range_attributes, 1)
                    ELSE NULL
                END AS numeric_range
            FROM {range_attributes}
            """,
        ),
    ]


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
