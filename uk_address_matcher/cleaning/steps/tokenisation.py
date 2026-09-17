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
    marker_alternatives = [
        (alternative, marker)
        for marker, pattern in marker_cases
        for alternative in pattern.split("|")
    ]
    marker_pattern = "|".join(alternative for alternative, _ in marker_alternatives)
    marker_sql = "\n".join(
        "WHEN "
        + " OR ".join(
            f"list_contains(__numeric_marker_matches[index], '{alternative}')"
            for alternative, alternative_marker in marker_alternatives
            if alternative_marker == marker
        )
        + f" THEN '{marker}'"
        for marker, _ in marker_cases
    )
    marker_matches_sql = (
        "regexp_extract_all("
        "clean_full_address, "
        f"concat('\\b({marker_pattern})\\s+', regexp_escape(token), '\\b'), "
        "1)"
    )
    return f"""
    WITH marker_matches AS (
        SELECT
            *,
            list_transform(
                numeric_tokens,
                token -> {marker_matches_sql}
            ) AS __numeric_marker_matches
        FROM {{input}}
    ),
    marked AS (
        SELECT
            *,
            list_transform(
                numeric_tokens,
                (token, index) -> CASE
                    {marker_sql}
                    ELSE 'ADDRESS_NUMBER'
                END
            ) AS __numeric_specific_markers
        FROM marker_matches
    )
    SELECT
        * EXCLUDE (__numeric_marker_matches, __numeric_specific_markers),
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
            AS address_without_numbers_tokenised,
        regexp_split_to_array(clean_full_address, '\\s+')::VARCHAR[]
            AS clean_full_address_tokens
    from {input}
    """
    return sql
