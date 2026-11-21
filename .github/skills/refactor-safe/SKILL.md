---
name: refactor-safe
description: "Perform a behaviour-preserving refactor with explicit boundaries and targeted validation."
disable-model-invocation: true
---
Perform the refactor without behavioural drift.

Success means:

- the refactor boundary is explicit before edits begin
- existing behaviour is anchored with nearby tests or other concrete checks
- public interfaces stay stable unless the user asked otherwise
- targeted validation is run after each meaningful slice where practical

Working rules:

- break the work into small, reviewable edits
- prefer local simplification over repo-wide cleanup
- preserve DuckDB/Splink pipeline behaviour and placeholders

Report:

- refactor boundary
- preserved behaviour evidence
- main internal simplification
- validation evidence

Constraints:
- No opportunistic rewrites.
- Maintain DuckDB/Splink pipeline behaviour and placeholders.
- Keep style PEP8, typed where practical, British spelling.
