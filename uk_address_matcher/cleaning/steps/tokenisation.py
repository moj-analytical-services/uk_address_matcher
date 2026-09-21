from __future__ import annotations

from uk_address_matcher.sql_pipeline.steps import CTEStep, pipeline_stage


@pipeline_stage(
    name="tokenise_clean_full_address",
    description="Split the finalized clean address into reusable tokens",
    tags="tokenisation",
)
def _tokenise_clean_full_address():
    return """
    SELECT
        *,
        regexp_split_to_array(clean_full_address, '\\s+')::VARCHAR[]
            AS clean_full_address_tokens
    FROM {input}
    """


@pipeline_stage(
    name="split_numeric_tokens_to_cols",
    description=(
        "Split numeric tokens array into separate columns "
        "(numeric_token_1, numeric_token_2, numeric_token_3)"
    ),
    tags="tokenisation",
)
def _split_numeric_tokens_to_cols():
    return [
        CTEStep(
            "with_numeric_scalars",
            """
            SELECT
                *,
                regexp_extract_all(
                    array_to_string(numeric_tokens, ' '),
                    '\\d+'
                ) AS __numeric_scalar_tokens
            FROM {input}
            """,
        ),
        CTEStep(
            "final",
            """
            SELECT
                * EXCLUDE (__numeric_scalar_tokens),
                __numeric_scalar_tokens[1] AS numeric_token_1,
                __numeric_scalar_tokens[2] AS numeric_token_2,
                __numeric_scalar_tokens[3] AS numeric_token_3
            FROM {with_numeric_scalars}
            """,
        ),
    ]


@pipeline_stage(
    name="derive_numeric_context_roles",
    description="Assign lightweight role markers to numeric tokens",
    tags="tokenisation",
)
def _derive_numeric_context_roles():
    marker_cases = [
        ("PARKING_SPACE", "PARKING SPACE|CAR PARK SPACE|CAR PARK"),
        ("CONTAINER", "CONTAINER"),
        ("PLATFORM", "PLATFORM"),
        ("MAST", "MAST"),
        ("PLOT", "PLOT"),
        ("GARAGE", "GARAGE"),
        ("YARD", "YARD"),
        ("SHOP", "SHOP"),
        ("BAY", "BAY"),
        ("FLOOR", "FLOOR|LEVEL"),
        (
            "UNIT",
            "UNIT|UNITS|SUITE|SUITES|OFFICE|ROOM|WORKSHOP|WAREHOUSE|STUDIO",
        ),
    ]

    def context_field(alternative: str) -> str:
        return {1: "one", 2: "two", 3: "three"}[len(alternative.split())]

    marker_priority_sql = "\n".join(
        "WHEN "
        + " OR ".join(
            f"context.previous_{context_field(alternative)} = '{alternative}'"
            for alternative in pattern.split("|")
        )
        + f" THEN {priority}::UTINYINT"
        for priority, (_marker, pattern) in enumerate(marker_cases, start=1)
    )
    marker_name_sql = "\n".join(
        f"WHEN {priority} THEN '{marker}'"
        for priority, (marker, _pattern) in enumerate(marker_cases, start=1)
    )
    return f"""
    WITH address_token_contexts AS (
        SELECT
            *,
            list_transform(
                range(1, len(clean_full_address_tokens) + 1),
                pos -> struct_pack(
                    token := list_extract(clean_full_address_tokens, pos),
                    previous_one := CASE
                        WHEN pos > 1 THEN list_extract(clean_full_address_tokens, pos - 1)
                        ELSE NULL::VARCHAR
                    END,
                    previous_two := CASE
                        WHEN pos > 2 THEN array_to_string(
                            list_slice(clean_full_address_tokens, pos - 2, pos - 1), ' '
                        )
                        ELSE NULL::VARCHAR
                    END,
                    previous_three := CASE
                        WHEN pos > 3 THEN array_to_string(
                            list_slice(clean_full_address_tokens, pos - 3, pos - 1), ' '
                        )
                        ELSE NULL::VARCHAR
                    END
                )
            ) AS __numeric_token_contexts
        FROM {{input}}
    ),
    address_marker_priorities AS (
        SELECT
            *,
            list_transform(
                __numeric_token_contexts,
                context -> CASE
                    {marker_priority_sql}
                    ELSE NULL::UTINYINT
                END
            ) AS __numeric_marker_priorities
        FROM address_token_contexts
    ),
    marker_entries AS (
        SELECT
            *,
            list_transform(
                list_filter(
                    range(1, len(__numeric_token_contexts) + 1),
                    pos -> list_extract(__numeric_marker_priorities, pos) IS NOT NULL
                ),
                pos -> struct_pack(
                    token := list_extract(__numeric_token_contexts, pos).token,
                    priority := list_extract(__numeric_marker_priorities, pos)
                )
            ) AS __numeric_marker_entries
        FROM address_marker_priorities
    ),
    marked AS (
        SELECT
            *,
            list_transform(
                numeric_tokens,
                token -> CASE list_min(list_transform(
                    list_filter(
                        __numeric_marker_entries,
                        entry -> entry.token = token
                    ),
                    entry -> entry.priority
                ))
                    {marker_name_sql}
                    ELSE 'ADDRESS_NUMBER'
                END
            ) AS __numeric_specific_markers
        FROM marker_entries
    )
    SELECT
        * EXCLUDE (
            __numeric_token_contexts,
            __numeric_marker_priorities,
            __numeric_marker_entries,
            __numeric_specific_markers
        ),
        list_transform(
            numeric_tokens,
            (token, index) -> CASE
                WHEN __numeric_specific_markers[index] = 'ADDRESS_NUMBER'
                    THEN 'location|ADDRESS_NUMBER|' || token
                WHEN __numeric_specific_markers[index] IN (
                    'PARKING_SPACE', 'CONTAINER', 'PLATFORM', 'MAST', 'PLOT',
                    'GARAGE', 'YARD', 'SHOP', 'BAY'
                )
                    THEN 'asset|' || __numeric_specific_markers[index] || '|' || token
                WHEN __numeric_specific_markers[index] = 'FLOOR'
                    THEN 'floor|FLOOR|' || token
                ELSE 'unit|UNIT|' || token
            END
        ) AS numeric_role_keys,
        list_transform(
            __numeric_specific_markers,
            marker -> CASE
                WHEN marker = 'ADDRESS_NUMBER' THEN 'location'
                WHEN marker IN (
                    'PARKING_SPACE', 'CONTAINER', 'PLATFORM', 'MAST', 'PLOT',
                    'GARAGE', 'YARD', 'SHOP', 'BAY'
                )
                    THEN 'asset'
                WHEN marker = 'FLOOR' THEN 'floor'
                ELSE 'unit'
            END
        ) AS numeric_broad_roles,
        __numeric_specific_markers AS numeric_specific_markers
    FROM marked
    """


@pipeline_stage(
    name="tokenise_address_without_numbers",
    description="Split the address_without_numbers field into an array of tokens",
    tags="tokenisation",
)
def _tokenise_address_without_numbers(*, use_precomputed_tokens: bool = False):
    if use_precomputed_tokens:
        return """
        SELECT
            * EXCLUDE (__address_without_numbers_tokenised),
            __address_without_numbers_tokenised AS address_without_numbers_tokenised
        FROM {input}
        """
    sql = """
    select
        *,
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {input}
    """
    return sql
