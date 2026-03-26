---
applyTo:
  - "uk_address_matcher/sql_pipeline/**/*.py"
  - "uk_address_matcher/linking_model/**/*.py"
  - "tests/**/*sql*.py"
  - "scripts/**/*.py"
  - "examples/**/*.py"
---

# DuckDB SQL pipeline conventions (uk_address_matcher)

- Keep SQL DuckDB-compatible; use explicit, descriptive CTE names.
- Be deliberate with aliases and projected fields; avoid ambiguous `SELECT *` unless justified.
- Window functions and `QUALIFY` are OK when they improve clarity.
- Keep pipeline stage logic inside the repo’s pipeline framework (do not bypass it).
- For staged pipeline logic:
  - declare stages with `@pipeline_stage`,
  - return `CTEStep` entries (prefer small, atomic steps),
  - reference dependencies with placeholders (e.g. `{annotated_exact_matches}`).
- Prefer pipeline helpers (e.g. step registration / queue helpers) over ad hoc execution.
- If introducing/changing match reasons, ensure `MatchReason` enum registration for DuckDB is updated.
- Preserve outputs required downstream; avoid accidental column drops or renamed columns without updating dependants.
- Missing-column failures usually indicate a stage dropped required outputs; verify `EXCLUDE` and projected columns.
- Enum cast failures usually indicate missing `MatchReason` DuckDB enum registration.
- Performance regressions often stem from poor join keys; prioritise postcode and canonical concatenation join paths.
- Only diverge from pipeline conventions when explicitly requested for experimentation.

- SQL stage change checklist:
  - Stage declared with `@pipeline_stage` and returns `CTEStep` entries.
  - Dependencies referenced using placeholders (for example `{annotated_exact_matches}`).
  - Required downstream columns preserved or dependent stages updated in the same change.
  - `MatchReason` updates include DuckDB enum registration updates.