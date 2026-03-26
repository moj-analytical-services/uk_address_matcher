---
applyTo:
  - "**"
---

# Testing expectations

- Any behavioural change should include test updates/additions.
- Prefer targeted tests near the changed component first, then broader regression checks as needed.
- Use project commands:
  - full suite: `uv run pytest`
  - targeted: `uv run pytest tests/<relevant_file>.py`
- Keep tests deterministic and readable; use fixtures in `example_data/` where possible.
- For SQL/pipeline changes, add assertions that catch lazy DuckDB/Splink failures.
- For SQL/pipeline changes, run tests that exercise the touched stages directly.
- Do not push changes without passing relevant local tests.
- Do not ship changes without reporting what tests were run and their outcome.

- Definition of done (testing):
  - Behavioural changes include matching test updates or additions.
  - Targeted tests for touched areas pass before broader checks.
  - Report includes commands run and outcomes.
