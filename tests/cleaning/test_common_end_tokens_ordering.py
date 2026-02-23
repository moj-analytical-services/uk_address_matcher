import json
from pathlib import Path

REORDER_INSTRUCTION = (
    "Token ordering is stale. Run ./shell/order_common_end_tokens_for_regex.sh "
    "from repository root and re-run tests."
)


def _normalise(token: str) -> str:
    return " ".join(token.strip().upper().split())


def _multi_sort_key(token: str) -> tuple[int, int, str]:
    return (-len(token.split()), -len(token), token)


def _single_sort_key(token: str) -> tuple[int, str]:
    return (-len(token), token)


def test_common_end_tokens_are_normalised_and_sorted(pytestconfig):
    json_path = (
        Path(pytestconfig.rootpath)
        / "uk_address_matcher"
        / "data"
        / "common_uk_end_tokens.json"
    )
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    multi_tokens = data.get("multi_tokens", [])
    single_tokens = data.get("single_tokens", [])

    assert multi_tokens == [_normalise(token) for token in multi_tokens], (
        "multi_tokens must be uppercased/normalised. " + REORDER_INSTRUCTION
    )
    assert single_tokens == [_normalise(token) for token in single_tokens], (
        "single_tokens must be uppercased/normalised. " + REORDER_INSTRUCTION
    )

    assert len(multi_tokens) == len(set(multi_tokens)), (
        "multi_tokens contains duplicates. " + REORDER_INSTRUCTION
    )
    assert len(single_tokens) == len(set(single_tokens)), (
        "single_tokens contains duplicates. " + REORDER_INSTRUCTION
    )

    assert multi_tokens == sorted(multi_tokens, key=_multi_sort_key), (
        "multi_tokens ordering is incorrect. " + REORDER_INSTRUCTION
    )
    assert single_tokens == sorted(single_tokens, key=_single_sort_key), (
        "single_tokens ordering is incorrect. " + REORDER_INSTRUCTION
    )


def test_common_end_token_aliases_are_normalised_and_sorted(pytestconfig):
    json_path = (
        Path(pytestconfig.rootpath)
        / "uk_address_matcher"
        / "data"
        / "common_uk_end_tokens.json"
    )
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    aliases = data.get("aliases", {})
    alias_keys = list(aliases.keys())

    assert alias_keys == [_normalise(key) for key in alias_keys], (
        "alias keys must be uppercased/normalised. " + REORDER_INSTRUCTION
    )
    assert list(aliases.values()) == [_normalise(value) for value in aliases.values()], (
        "alias values must be uppercased/normalised. " + REORDER_INSTRUCTION
    )
    assert alias_keys == sorted(alias_keys, key=_multi_sort_key), (
        "alias key ordering is incorrect. " + REORDER_INSTRUCTION
    )
