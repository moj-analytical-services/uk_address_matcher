old_blocking_rules = [
    """
    l.numeric_token_1 = r.numeric_token_1
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)
    AND list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 2)
    AND split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)
    """,
    """
    l.numeric_token_1 = r.numeric_token_2
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)
    """,
    """
    l.numeric_token_1 = r.numeric_token_2
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)
    """,
    """
    l.numeric_token_1 = r.numeric_token_1
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2)
    AND list_extract(l.unusual_tokens_arr, 2) = list_extract(r.unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)
    """,
    """
    l.numeric_token_1 = r.numeric_token_1
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 2)
    AND split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)
    """,
    "l.numeric_token_1 = r.numeric_token_1 and l.postcode = r.postcode",
    "l.numeric_token_1 = r.numeric_token_2 and l.postcode = r.postcode",
    (
        "list_extract(l.unusual_tokens_arr, 1) = "
        "list_extract(r.unusual_tokens_arr, 2) and l.postcode = r.postcode"
    ),
    (
        "list_extract(l.very_unusual_tokens_arr, 1) = "
        "list_extract(r.very_unusual_tokens_arr, 1) and "
        "l.numeric_token_1 = r.numeric_token_1"
    ),
    (
        "list_extract(l.very_unusual_tokens_arr, 1) = "
        "list_extract(r.very_unusual_tokens_arr, 2) and "
        "l.numeric_token_1 = r.numeric_token_1"
    ),
    """
    l.numeric_token_2 = r.numeric_token_2
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)
    """,
    """
    l.numeric_token_1 = r.numeric_token_1
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)
    """,
    """
    l.numeric_token_2 = r.numeric_token_2
    AND list_extract(l.unusual_tokens_arr, 1) = list_extract(r.unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)
    """,
    "l.numeric_token_2 = r.numeric_token_2 and l.postcode = r.postcode",
    # "l.numeric_token_1 = r.numeric_1_alt and l.postcode = r.postcode",
    # "l.numeric_1_alt = r.numeric_token_1 and l.postcode = r.postcode",
    # "l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2"
    # " and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)",
    # "l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2"
    # " and split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)",
    # "l.numeric_token_1 = r.numeric_1_alt and l.numeric_token_2 = r.numeric_token_2"
    # " and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)",
    # "l.numeric_1_alt = r.numeric_token_1 and l.numeric_token_2 = r.numeric_token_2"
    # " and split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)",
    """
    l.numeric_token_1 = r.numeric_token_1
    AND l.numeric_token_2 = r.numeric_token_2
    AND split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)
    """,
    """
    l.numeric_token_1 = r.numeric_token_1
    AND l.numeric_token_2 = r.numeric_token_2
    AND split_part(l.postcode, ' ', 2) = split_part(r.postcode, ' ', 2)
    """,
    """
    list_extract(l.extremely_unusual_tokens_arr, 1) =
        list_extract(r.extremely_unusual_tokens_arr, 1)
    AND split_part(l.postcode, ' ', 1) = split_part(r.postcode, ' ', 1)
    """,
]
