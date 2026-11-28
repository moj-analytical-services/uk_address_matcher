---
name: pre-push
description: "Required before pushing any branch or opening or updating a PR. Verify lint, the full test suite, and the scope of matching-model changes."
---

# Pre-push gate

1. Review the outgoing diff. Keep it minimal and exclude unrelated changes and
   local configuration. Do not commit generated charts or experiment-specific
   documentation unless the author explicitly requests those files; a link in
   PR text is not permission. Keep PR summaries brief and avoid redundant changelogs.
2. Keep changes to model execution in the DuckDB SQL pipeline. Put new parsing
   or cleaning rules in `uk_address_matcher/cleaning/`, model settings in
   `uk_address_matcher/data/splink_model.json`, and reranking logic in reranker
   stages. Add or edit a SQL pipeline stage for deterministic matching rules.
   Do not add a parallel Python scoring or matching path.
3. Run these checks on the exact revision to be pushed:

   ```sh
   uv run --locked --group linting ruff check uk_address_matcher tests benchmarking
   uv run --locked --group linting ruff format --check uk_address_matcher tests benchmarking
   RUN_LABELLING_BROWSER_TESTS=1 uv run --locked --group model-training --group security pytest
   ```

4. Push only if all three commands pass. The full suite needs Node.js and
   Playwright Chromium; missing prerequisites are blockers, not a reason to
   skip tests. Report the commands and outcomes; if any check is blocked or
   fails, report why and do not push. Re-run checks if the revision changes.
