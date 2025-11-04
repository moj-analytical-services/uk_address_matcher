# Agents guide

This document acts as your onboarding pack when working in the `uk_address_matcher` repository. It explains the project purpose, key components, workflows, and the house rules you must follow in both Python and Markdown.

## Project overview

- Goal: match messy UK address records to a canonical gazetteer with high precision and speed.
- Core tools: DuckDB SQL, the Splink probabilistic linkage model, and Python helper utilities.
- Typical flow:
  1. Clean input data to generate token features.
  2. Run deterministic exact matching to remove easy wins.
  3. Execute Splink based probabilistic matching and apply distinguishing token analysis.
  4. Produce final match decisions and metrics for downstream reporting.

> [! TIP]
> Keep a DuckDB connection available. The package leans heavily on SQL pipelines, so testing or debugging often happens inside DuckDB relations rather than pure Pandas.

## Repository structure

- `uk_address_matcher/`
  - `cleaning/`: Address parsing, token generation, and shared preprocessing logic.
  - `linking_model/`: Matching stages. Notable subpackages include `exact_matching` for deterministic joins and `post_linkage` for downstream metrics.
  - `sql_pipeline/`: Pipeline framework that composes DuckDB CTE stages with dependency tracking and match reason enums.
- `examples/`: End to end scripts demonstrating address cleaning, matching, and evaluation.
- `scripts/`: Utility programmes for training, analysis, and parameter tuning.
- `tests/`: Pytest based regression coverage for SQL stages, cleaning rules, and integration scenarios.
- `example_data/`: Small parquet fixtures used by demos and tests.
- `pyproject.toml`: Official dependency and tooling definition. The project adheres to modern Python packaging standards.

## Environment and tooling

- Package manager: [uv](https://github.com/astral-sh/uv).
  - Create or refresh the environment with `uv sync`.
  - Run ad hoc commands inside the environment with `uv run ...` (for example `uv run python scripts/run_training.py`).
- Python version: see `pyproject.toml` (`requires-python` >= 3.9).
- Coding standard: PEP8 with type hints where practical. Use British English spelling in identifiers, comments, and documentation. Do not construct code with headers and page delineators unless explicitly told to do so.
- SQL style: favour explicit CTE names, avoid ambiguous field selection, and keep all DuckDB SQL compatible. Window functions and `QUALIFY` are already embraced.

> [! NOTE]
> When adding new Python dependencies, update `pyproject.toml`, run `uv lock` or `uv sync`, and commit the refreshed `uv.lock`.

## Development workflow

1. Create a feature branch from `main`.
2. Install project dependencies via uv if not already done.
3. Implement changes following PEP8 and repository conventions.
4. Add or update Pytest coverage for new behaviours.
5. Run the test suite before committing.

### Running tests

- Full suite: `uv run pytest`.
- Targeted file: `uv run pytest tests/test_exact_matching.py`.

> [! IMPORTANT]
> Tests must pass locally before you push. Splink pipelines and DuckDB SQL can fail lazily, so exercise the stages you touch.

## Data handling guidelines

- Input relations use the schema documented in `general_context.md` (columns such as `unique_id`, `address_concat`, and `postcode`).
- Canonical datasets are often deduplicated gazetteers. Deterministic stages rely on stable tokens and normalised concatenations.
- Do not commit or check in real personal data. Use the parquet fixtures in `example_data/` for reproductions.

## SQL pipeline notes

- Stages are declared with `@pipeline_stage` and return `CTEStep` entries.
- The pipeline runner handles alias mapping. Always reference prior outputs via placeholders (for example `{annotated_exact_matches}`).
- Match reasons use the `MatchReason` enum. Ensure any new enum values are registered through the helper so DuckDB knows about the ENUM type.
- For free form SQL, prefer `register_step` with the pipeline queuer utilities in `uk_address_matcher.sql_pipeline`. This queues CTE fragments, keeps the plan inspectable, and supports stepwise debugging.

> [! WARNING]
> DuckDB macros or functions must be registered before running pipelines that reference them. Tests often stub `build_suffix_trie` and `find_address` for this reason.

## Python coding style preferences

- Follow PEP8 and use functions wherever practical; reach for classes only when they genuinely communicate stateful behaviour (settings containers, data structures, or clear interfaces such as the pipeline `Stage` definitions in `sql_pipeline/steps.py`).
- When writing throwaway scripts for experiments or validation, keep them as straight scripts; avoid `if __name__ == "__main__"` to ease debugging. Production entry points can still use the guard.

## Commit and review conventions

- Use conventional commits in the form `type(scope): summary`. The `scope` is optional but
  strongly encouraged when we touch a distinct package or feature flag.
- Keep the summary to 72 characters or fewer so it renders cleanly in terminals.
- Follow the quick primer from [qoomon’s cheat sheet](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13):
  - Choose a recognised `type` (`feat`, `fix`, `refactor`, `docs`, `test`, `chore`, etc.).
  - Leave a blank line between the summary and the body if you need one.
  - When a change spans multiple areas or ~50+ lines, add a bulleted body summarising the
    key edits (one bullet per logical chunk) before any ticket references.
- Reference tickets or issues in the body where applicable, after the summary/bullets.
- Keep pull requests focused and link to relevant tests.

> [! CAUTION]
> Avoid auto generated commit messages. Review the scope and ensure the summary uses British spelling.

## Documentation standards

- Write Markdown with British English and use the alert pattern shown above for hints.
- Footnotes follow the format `[^1]` within the text, with definitions placed at the end of the document.
- Keep line length manageable for diff readability (wrap around 100 characters).
- Prefer fenced code blocks with language hints, for example ```python or ```bash.

## Troubleshooting checklist

- Pipelines failing with missing columns usually mean a stage dropped required fields. Revisit the `EXCLUDE` clauses.
- Enum casting errors imply the DuckDB ENUM was not registered via `MatchReason.ensure_duckdb_enum`.
- Performance issues often trace back to missing indices on join keys. Ensure postcode and concatenated address columns are present and pre ordered when appropriate.

> [! TIP]
> Use the pipeline `debug` mode to inspect intermediate CTE outputs quickly. It can materialise each stage and show the SQL that DuckDB executes.

## Future reference overview

This project marries deterministic SQL stages with probabilistic Splink linkage to achieve rapid UK address matching. Core responsibilities:

- `cleaning`: create standardised tokens and representations of addresses.
- `linking_model.exact_matching`: deterministic postcode plus address concat matching, followed by trie based fallbacks.
- `linking_model.post_linkage`: evaluate match quality and derive metrics.
- `sql_pipeline`: orchestrate staged DuckDB execution with dependency management and match reason enums.

Tests under `tests/` cover everything from cleaning edge cases to full matching flows. Examples and scripts provide runnable demonstrations. When extending the codebase, update this guide if you introduce new workflows or tooling.

[^1]: For long running experiments, prefer background uv tasks and document their commands in the relevant markdown files.
