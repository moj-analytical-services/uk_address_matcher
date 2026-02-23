#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TOKENS_PATH="${1:-${REPO_ROOT}/uk_address_matcher/data/common_uk_end_tokens.json}"

python3 - <<'PY' "${TOKENS_PATH}"
import json
import sys
from pathlib import Path


def normalise_token(token: str) -> str:
    return " ".join(token.strip().upper().split())


def dedupe_normalised(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        token = normalise_token(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def sort_multi(values: list[str]) -> list[str]:
    return sorted(values, key=lambda token: (-len(token.split()), -len(token), token))


def sort_single(values: list[str]) -> list[str]:
    return sorted(values, key=lambda token: (-len(token), token))


def sort_aliases(aliases: dict[str, str]) -> dict[str, str]:
    normalised_pairs = {}
    for key, value in aliases.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        normalised_key = normalise_token(key)
        normalised_value = normalise_token(value)
        if not normalised_key or not normalised_value:
            continue
        normalised_pairs[normalised_key] = normalised_value

    return {
        key: normalised_pairs[key]
        for key in sorted(
            normalised_pairs,
            key=lambda token: (-len(token.split()), -len(token), token),
        )
    }


path = Path(sys.argv[1]).resolve()
data = json.loads(path.read_text(encoding="utf-8"))

aliases = sort_aliases(data.get("aliases", {}) or {})
multi_tokens = sort_multi(dedupe_normalised(list(data.get("multi_tokens", []) or [])))
single_tokens = sort_single(
    dedupe_normalised(list(data.get("single_tokens", []) or []))
)

ordered = {
    "aliases": aliases,
    "multi_tokens": multi_tokens,
    "single_tokens": single_tokens,
}

path.write_text(json.dumps(ordered, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"Updated {path}")
print(
    f"aliases={len(aliases)} multi_tokens={len(multi_tokens)} single_tokens={len(single_tokens)}"
)
PY
