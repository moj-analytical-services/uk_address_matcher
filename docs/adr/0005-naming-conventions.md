# 5. naming-conventions

Date: 2026-02-06

## Status

Proposed

## Context
Following https://github.com/moj-analytical-services/uk_address_matcher/issues/181, https://github.com/moj-analytical-services/uk_address_matcher/pull/144 and other recent discussions, naming has diverged across the codebase. We
currently mix terms such as messy, fuzzy, canonical, deterministic, probabilistic, pass, stage,
and phase. Several public API parameters and post linkage helpers use inconsistent terms, and
match reason enum values mix styles. These gaps create confusion for users and complicate
documentation ahead of 1.0.

## Decision
Formalise naming conventions and update documentation to align with the terminology below.

### Address table parameters

Standardise parameter names for address inputs:

| Location | Current names | Proposed |
|----------|---------------|----------|
| `run_deterministic_match_pass` | `df_addresses_to_match` / `df_addresses_to_search_within` | `df_messy` / `df_canonical` |
| `get_linker` | `df_addresses_to_match` / `df_addresses_to_search_within` | `df_messy` / `df_canonical` |
| `select_top_match_candidates` | `df_exact_matches` / `df_splink_matches` | `df_high_precision_matches` / `df_probabilistic_matches` |

### Address tables

Use consistent table names across examples and documentation:
- `messy_addresses` for records to be matched.
- `canonical_addresses` for the canonical reference list.
- `__ukam_results` for the outputs table, as referenced in #181.

### Phases and stages

Adopt a clear hierarchy:
- Phase: top level matching phase (high precision pass, probabilistic pass).
- Stage: sub steps within a phase (exact stage, trigram stage within the high precision phase).

Rename linkage phases to describe intent:
- `run_high_precision_match_pass` replaces deterministic naming.
- `run_probabilistic_match_pass` replaces the current linker pass naming.

### Match reason enum values

Standardise on `"{pass}:{stage}"` for `match_reasons.py` values:
- `"high_precision:exact"`
- `"high_precision:trigram"`
- `"probabilistic:splink"`

This is a breaking change for any consumers who parse match reasons.

### Splink output suffixes

Document the suffix mapping for Splink outputs:
- `_r` = messy (right dataset in Splink)
- `_l` = canonical (left dataset in Splink)

This mapping is driven by `input_table_aliases=["m_", "c_"]` in `splink_model.py`, but is
not obvious to users. If feasible, adjust suffixes so probabilistic outputs are easier to
interpret for messy vs canonical fields.

### Post linkage function naming

If post linkage helpers remain public, introduce a consistent prefix such as `postprocess_*`
for functions like `best_matches_with_distinguishability`,
`improve_predictions_using_distinguishing_tokens`, and `select_top_match_candidates`.

## Consequences
Clearer API and documentation that align with messy and canonical terminology, improving
user comprehension and reducing inconsistent naming across examples. The match reason enum
format change is a breaking change for downstream parsing and must be communicated ahead of
release. Renaming functions and parameters will require updates to public API references,
examples, and tests, plus deprecation messaging where appropriate.
