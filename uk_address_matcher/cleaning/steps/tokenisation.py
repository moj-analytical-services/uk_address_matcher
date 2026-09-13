from __future__ import annotations

from uk_address_matcher.sql_pipeline.steps import pipeline_stage


@pipeline_stage(
    name="split_numeric_tokens_to_cols",
    description=(
        "Split numeric tokens array into separate columns "
        "(numeric_token_1, numeric_token_2, numeric_token_3)"
    ),
    tags="tokenisation",
)
def _split_numeric_tokens_to_cols():
    sql = """
    SELECT
        *,
        regexp_extract_all(
            array_to_string(numeric_tokens, ' '),
            '\\d+'
        )[1] as numeric_token_1,
        regexp_extract_all(
            array_to_string(numeric_tokens, ' '),
            '\\d+'
        )[2] as numeric_token_2,
        regexp_extract_all(
            array_to_string(numeric_tokens, ' '),
            '\\d+'
        )[3] as numeric_token_3
    FROM {input}
    """
    return sql


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
    marker_sql = " ".join(
        "WHEN regexp_matches(clean_full_address, concat("
        f"'\\b({pattern})\\s+', regexp_escape(token), '\\b')) "
        f"THEN '{marker}'"
        for marker, pattern in marker_cases
    )
    return f"""
    WITH marked AS (
        SELECT
            *,
            list_transform(
                numeric_tokens,
                token -> CASE
                    {marker_sql}
                    ELSE 'ADDRESS_NUMBER'
                END
            ) AS __numeric_specific_markers
        FROM {{input}}
    )
    SELECT
        * EXCLUDE (__numeric_specific_markers),
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
def _tokenise_address_without_numbers():
    sql = """
    select
        *,
        regexp_split_to_array(trim(address_without_numbers), '\\s+')
            AS address_without_numbers_tokenised
    from {input}
    """
    return sql
