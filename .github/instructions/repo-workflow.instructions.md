---
applyTo:
  - "**/*.py"
  - "**/*.sql"
  - ".github/**/*.md"
  - "docs/developer/**/*.md"
  - "docs/findings/**/*.md"
  - "CHANGELOG.md"
  - "pyproject.toml"
  - "shell/**/*.sh"
  - ".github/workflows/**/*.yml"
  - ".github/workflows/**/*.yaml"
---

# Repository workflow and change hygiene

- Purpose: match messy UK addresses to a canonical gazetteer with high precision and speed.
- Core stack: Python, DuckDB SQL pipelines, and Splink.
- Key areas: `uk_address_matcher/cleaning/`, `uk_address_matcher/linking_model/`,
  `uk_address_matcher/sql_pipeline/`, plus `tests/`, `examples/`, and `scripts/`.
- Use project tooling via `uv`:
  - `uv sync` to create or refresh the environment.
  - `uv run <command>` for test and script execution.

- Engineering principles:
  - Keep interfaces explicit and behaviour readable; avoid black-box side effects.
  - Prefer minimal, composable changes that reduce downstream user work.
  - Respect existing workflows and integrations; avoid forcing new patterns without need.

## Change policy

- Make the smallest possible change that fully satisfies the explicit request.
- Change only code directly required by the request, modifying the fewest files and lines possible.
- Do not perform unrelated refactoring, cleanup, formatting, renaming, dependency upgrades,
  documentation rewrites, or architectural changes.
- Do not introduce abstractions, helpers, dependencies, configuration, or files unless necessary
  for the requested behaviour; follow existing project patterns and preserve surrounding comments
  and formatting.
- Preserve current behaviour outside the request. Prefer local fixes over system-wide redesigns;
  avoid speculative handling for hypothetical requirements.
- Do not modify generated files, vendored code, lockfiles, migrations, or build output unless the
  request requires it.
- Add or update only focused tests for the requested behaviour; do not rewrite tests to accommodate
  an unintended behaviour change.

### Public API stability

- Treat the public API as frozen unless the request explicitly authorises a change. This includes
  public classes, functions, methods, properties, constants, types, signatures, package exports,
  module paths, routes, request and response formats, CLI behaviour, configuration, environment
  variables, schemas, serialised data, error types, side effects, defaults, and runtime behaviour.
- Do not rename, move, remove, reorder, narrow, or otherwise change a public API or existing
  default merely to make an implementation cleaner.
- When work appears to require a breaking API change, do not make it. Briefly explain the conflict,
  propose a backward-compatible alternative, and wait for explicit authorisation.

### Communication

- Before making changes, briefly state which files need to change and why.
- After making changes, report only what changed, checks performed, and any unresolved limitation.
- When instructions are ambiguous, choose the interpretation that preserves current behaviour and
  produces the smallest diff.

- Start from a feature branch off `main`.
- Keep diffs focused; avoid unrelated formatting/refactor churn.
- Prefer the smallest viable implementation unless the user asks for a larger feature or experiment.
- Touch as few files as possible to deliver the requested outcome safely.
- Non-goals by default:
  - Do not refactor unrelated modules while implementing a scoped change.
  - Do not change public APIs without explicit request.
  - Do not introduce new dependencies when existing utilities are sufficient.
- Add or update unit tests when behaviour changes and tests are applicable.
- Tests should pass before push.
- Use conventional commits: `type(scope): summary` (British spelling, concise summary).
- Keep commit summaries to 72 characters or fewer.
- Avoid auto-generated commit messages; write clear intent and scope.
- If change size is substantial, include a short commit body with key bullets.
- Link PRs/issues when relevant and include test evidence in PR descriptions.
- For larger changes, include bullets that map to logical chunks of work.
- If dependencies change, update `pyproject.toml` and refresh `uv.lock`.
