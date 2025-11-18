from typing import Callable, List


def remove_commas_periods(input: str):
    return f"regexp_replace({input}, '[,.]', ' ', 'g')"


def remove_apostrophes(input: str):
    return f"regexp_replace({input}, e'\\'', '', 'g')"


def remove_multiple_spaces(input: str):
    return f"regexp_replace({input}, '\\s+', ' ', 'g')"


def standarise_num_dash_num(input: str):
    """
    Standardizes numeric ranges with dashes by removing spaces around the dash.

    Examples:
        '23A - 24' -> '23A-24'
        '230-234' -> '230-234'
        '1 - 123' -> '1-123'
        'WELLS-NEXT-THE-SEA' -> 'WELLS-NEXT-THE-SEA' (unchanged)

    Args:
        input (str): The input string to process.

    Returns:
        str: The standardized string with numeric ranges formatted correctly.
    """
    regex_pattern = (
        r"(?<![A-Za-z])"  # Negative lookbehind to ensure the preceding character is not a letter
        r"(\d+[A-Za-z]?)"  # Matches a number with an optional single letter
        r"\s*-\s*"  # Matches spaces around a dash
        r"(\d+[A-Za-z]?)"  # Matches another number with an optional single letter
        r"(?![A-Za-z])"  # Negative lookahead to ensure the following character is not a letter
    )
    return f"regexp_replace({input}, '{regex_pattern}', '\\1-\\2', 'g')"


# TODO(ThomasHepworth): Do we want this...? For Scottish flats we actually want to keep the slash
def replace_fwd_slash_with_dash(input: str):
    # Sometimes we see Unit 5/6 as opposed to unit 5-6
    return f"regexp_replace({input}, '/', '-', 'g')"


# TODO:  PROBABLY NO LONGER NEEDED
def remove_repeated_tokens(input: str):
    """
    If a token that's at least four characters long is repeated, remove the second instance.

    Examples:
        'word word' -> 'word'
        'test test' -> 'test'
        'hello hello world' -> 'hello world'
        'this this is is a a test test' -> 'this is a test'

    Args:
        input (str): The input string to process.

    Returns:
        str: The string with repeated tokens removed.
    """
    regex_pattern = (
        r"\b"  # Word boundary
        r"(\w{4,})"  # Matches a word that is at least four characters long
        r"\s+"  # Matches one or more whitespace characters
        r"\1"  # Matches the same word captured in the first group
        r"\b"  # Word boundary
    )
    return f"regexp_replace({input}, '{regex_pattern}', '\\1', 'g')"


def trim(input: str):
    return f"trim({input})"


def standarise_num_letter(input: str):
    """
    Matches a number (1 to 4 digits) followed by any punctuation or space,
    followed by a single letter (A-Z or a-z) and a space, and replaces it with
    the number followed directly by the letter and the space.

    Examples:
        123-A -> 123A
        456 B -> 456B
        78.C -> 78C
        90/ d -> 90d
        1234 - E -> 1234E

    Args:
        input (str): The input string to standardize.

    Returns:
        str: The standardized string.
    """
    regex_pattern = (
        r"(\d{1,4})"  # Matches a number with 1 to 4 digits
        r"[[:punct:]\s]"  # Matches any punctuation or space
        r"([A-Za-z])"  # Matches a single letter (A-Z or a-z)
        r"\s"  # Matches a space
    )
    return f"regexp_replace({input}, '{regex_pattern}', '\\1\\2 ', 'g')"


def separate_letter_num(input: str):
    """
    Matches a single letter followed by a number (with or without spaces in between),
    and replaces it with the letter followed by a space and the number.

    Examples:
        'C230' -> 'C 230'
        'C 230' -> 'C 230'
        'C  230' -> 'C 230'

    Args:
        input (str): The input string to process.

    Returns:
        str: The processed string with the letter and number separated by a space.
    """
    regex_pattern = (
        r"\b"  # Word boundary
        r"([A-Za-z])"  # Matches a single letter (A-Z or a-z)
        r"\s*"  # Matches zero or more whitespace characters
        r"(\d+)"  # Matches one or more digits
        r"\b"  # Word boundary
    )
    return f"regexp_replace({input}, '{regex_pattern}', '\\1 \\2', 'g')"


def move_flat_to_front(input: str):
    """
    Move reference to a 'flat + number' to the front of the string address string,
    irrespective of where it exists in the string.

    Examples:
        'MY HOUSE FLAT 1A TOWN CITY' -> 'FLAT 1A MY HOUSE TOWN CITY'
        '123 MAIN STREET FLAT 2B' -> 'FLAT 2B 123 MAIN STREET'

    Args:
        input (str): The input string to process.

    Returns:
        str: The string with 'flat + number' moved to the front.
    """
    regex_pattern = (
        r"\b"  # Word boundary
        r"(FLAT \d+[A-Z]?)"  # Matches 'FLAT ' followed by a number and optional letter
        r"\s+"  # Matches one or more whitespace characters
        r"(.*)"  # Matches the rest of the string
    )
    return f"regexp_replace({input}, '{regex_pattern}', '\\1 \\2', 'g')"


def construct_nested_call(col_name: str, fns: List[Callable]) -> str:
    input_str = col_name
    for f in fns:
        input_str = f(input_str)
    return input_str
