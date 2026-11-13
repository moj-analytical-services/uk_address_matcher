# Cleaning package overview

This folder contains the reusable building blocks for cleaning, standardising, and feature‑engineering address‑like text prior to matching and model consumption. It is intentionally modular: small, composable steps live under `steps/`, and light orchestration lives in `pipelines.py` with a minimal public API re‑exported from `__init__.py`.

Quick links (relative):
- Orchestration: [`pipelines.py`](./pipelines.py)
- Public API re‑exports: [`__init__.py`](./__init__.py)
- Steps:
  - Normalisation: [`steps/normalisation.py`](./steps/normalisation.py)
  - Tokenisation: [`steps/tokenisation.py`](./steps/tokenisation.py)
  - Token parsing (feature extraction): [`steps/token_parsing.py`](./steps/token_parsing.py)
  - Term frequencies / weighting: [`steps/term_frequencies.py`](./steps/term_frequencies.py)
  - Shared regexes: [`steps/regexes.py`](./steps/regexes.py)

## Cleaning stages (what each stage aims to do)

The package supports four distinct but complementary stages. Each stage has a clear goal and a concrete implementation file.

1) Normalisation - inputs → canonical strings
   - Goal: produce a consistent, comparable text form before token handling.
   - Operations:
     - Normalise case (project standard is upper case).
     - Trim and collapse whitespace; control punctuation consistently.
     - Apply postcode hygiene (upper case and trimmed). For matching, prefer removing the postcode from addresses using a literal, case‑insensitive replace to avoid regex pitfalls.
     - Optionally expand abbreviations (e.g., RD → ROAD) using project dictionaries.
   - Implementation: [`steps/normalisation.py`](./steps/normalisation.py)

2) Tokenisation - canonical strings → tokens
   - Goal: create deterministic token arrays (`VARCHAR[]`) suitable for downstream exact searches and weighting.
     - Split on spaces after normalisation; collapse duplicate whitespace.
     - Apply a consistent policy on punctuation, hyphens, and separators (documented in code comments).
   - Implementation: [`steps/tokenisation.py`](./steps/tokenisation.py)

3) Token parsing (feature extraction) - tokens/strings → structured features
   - Goal: extract useful features from address text to support modelling or rules.
   - Outputs: `country`, `county`, `post_town`, `street_name`, `street_type`, `building_number`, `sub_building` (unit/flat), `po_box`.
   - Notes: individual parsing utilities are internal; expose a single aggregator (e.g., `parse_features(...)`) for callers.
   - Implementation: [`steps/token_parsing.py`](./steps/token_parsing.py)

4) Term‑frequency adjustments - tokens → weighted/filterable tokens
   - Goal: reduce the influence of very common tokens during scoring or search.
   - Operations:
     - Compute or load token frequencies; down‑weight common tokens rather than dropping them outright where possible.
     - Optionally filter extreme stop words based on configuration.
   - Implementation: [`steps/term_frequencies.py`](./steps/term_frequencies.py)

Orchestration and composition
- Thin pipelines (e.g., clean → tokenise) live in [`pipelines.py`](./pipelines.py); they compose the stages above with minimal logic and return relations/tables suitable for further SQL.
- The stable public API (re‑exported in [`__init__.py`](./__init__.py)) exposes high‑level entry points such as `clean_and_tokenise_inputs(...)`. Step‑level utilities remain private to allow internal iteration without breaking callers.

## Sub-premise evidence

`_parse_out_sub_premise` extracts explicit flat and business-unit identifiers once;
floor and side/location evidence remain separate. Matching compares these prepared
fields rather than reparsing full addresses. Match structs and address tails are
temporary. `sub_premise_explicit_identifier` preserves the explicit identifier
independently of legacy flat evidence: `FLAT A 1` has explicit identifier `A1`
and legacy flat letter `A`. Canonical storage retains `flat_number` and
`flat_letter`, because they cannot be losslessly recovered from the explicit
identifier. Identities, business-unit fields, flags and floor-prefix lists are
recomputed on load. `sub_premise_building_number` records a unique building
number in the remaining address, rejecting nested descriptors and multiple
numbers; older bundles use null for new explicit fields until rebuilt.
