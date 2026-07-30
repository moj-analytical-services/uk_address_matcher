from __future__ import annotations

TOKEN_IDF_QUANTISATION_SCALE = 256


def shared_token_information_q_sql(
    left_column: str = "token_idf_q_hist_l",
    right_column: str = "token_idf_q_hist_r",
) -> str:
    """Return the quantised shared-token information SQL expression."""
    return f"""
        COALESCE(
            list_sum(
                list_transform(
                    map_entries({left_column}),
                    entry ->
                        CASE
                            WHEN {right_column}[entry.key] IS NOT NULL
                            THEN
                                CAST(
                                    LEAST(
                                        entry.value.idf_q,
                                        ({right_column}[entry.key]).idf_q
                                    )
                                    AS UBIGINT
                                )
                                *
                                CAST(
                                    LEAST(
                                        entry.value.token_count,
                                        ({right_column}[entry.key]).token_count
                                    )
                                    AS UBIGINT
                                )
                            ELSE 0::UBIGINT
                        END
                )
            ),
            0::UBIGINT
        )
    """.strip()


def quantised_shared_token_comparison() -> dict:
    """Build the reduced-band quantised shared-token comparison."""
    score_sql = shared_token_information_q_sql()
    levels = [
        (6144, "Shared-token information > 24", 32768.0),
        (4096, "Shared-token information > 16", 8192.0),
        (3072, "Shared-token information > 12", 4096.0),
        (2048, "Shared-token information > 8", 256.0),
        (1024, "Shared-token information > 4", 16.0),
        (256, "Shared-token information > 1", 2.0),
        (0, "Some shared-token information", 1.0),
    ]
    return {
        "output_column_name": "shared_token_information",
        "comparison_description": (
            "Quantised information supplied by shared nonnumeric address tokens"
        ),
        "comparison_levels": [
            {
                "sql_condition": (
                    "token_idf_q_hist_l IS NULL OR token_idf_q_hist_r IS NULL"
                ),
                "label_for_charts": "Shared-token information is null",
                "is_null_level": True,
            },
            *[
                {
                    "sql_condition": f"({score_sql}) > {threshold}",
                    "label_for_charts": label,
                    "m_probability": m_probability,
                    "u_probability": 1.0,
                    "fix_m_probability": True,
                    "fix_u_probability": True,
                }
                for threshold, label, m_probability in levels
            ],
            {
                "sql_condition": "ELSE",
                "label_for_charts": "No shared nonnumeric tokens",
                "m_probability": 0.5,
                "u_probability": 1.0,
                "fix_m_probability": True,
                "fix_u_probability": True,
            },
        ],
    }
