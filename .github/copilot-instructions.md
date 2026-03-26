# Copilot instructions for `uk_address_matcher`

## Canonical source
- Treat files under `.github/instructions/` as canonical for repository-specific conventions.
- If there is any conflict, prefer the most specific matching `.instructions.md` file.
- Re-check relevant instruction files before major edits, SQL pipeline changes, or PR-ready summaries.

## Task routing
- For SQL stage and linking-model changes, read `.github/instructions/sql-duckdb-pipeline.instructions.md` first.
- For Python implementation details, read `.github/instructions/python-style.instructions.md` first.
- For test changes and validation evidence, read `.github/instructions/testing.instructions.md` first.
- For fixtures, privacy, and schema-safety concerns, read `.github/instructions/data-handling.instructions.md` first.
- For workflow, scope control, and commit hygiene, read `.github/instructions/repo-workflow.instructions.md` first.
- Detailed behavioural rules, checklists, and coding standards should be taken from those instruction files instead of duplicated here.

## Project defaults
- Purpose: match messy UK addresses to a canonical gazetteer with precision and speed.
- Core stack: Python + DuckDB SQL pipelines + Splink.
- Key areas: `uk_address_matcher/cleaning/`, `uk_address_matcher/linking_model/`,
  `uk_address_matcher/sql_pipeline/`, plus `tests/`, `examples/`, `scripts/`, `example_data/`.
- Always use `uv` to run Python commands: `uv sync` to refresh the environment, `uv run <command>` to execute scripts and tests (e.g. `uv run pytest`).