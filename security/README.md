# Developer security administration

This directory is maintained exclusively by developers. Contributors run checks but
must not change its contents. Runtime users do not need these tools or files.

## Local checks

```bash
uv sync --locked --group security --group linting
uv run --no-sync pre-commit install --config security/pre-commit-config.yaml
uv run --no-sync pre-commit run --all-files --config security/pre-commit-config.yaml
npm --prefix frontend/labelling audit --package-lock-only --ignore-scripts --audit-level=high
uv run --no-sync pytest tests/test_security_checks.py
```

Reinstall existing hooks with the command above after the configuration moves.

The secret checker copies the maintained baseline into a temporary directory. Metadata
updates happen only in that disposable copy; neither contributor hooks nor CI update
the original. Bandit blocks medium-or-higher severity findings at medium-or-higher
confidence. Python audits cover the locked graph, including development and docs
groups, without installing audited packages. JavaScript audits include development
dependencies and do not execute lifecycle scripts. Weekly CI rechecks new advisories.

Bandit scans only Python files under `uk_address_matcher/`, `benchmarking/` and
`tests/`. It remains enabled locally but is temporarily skipped in CI through
`SKIP: bandit` in `.github/workflows/security.yml`. To restore it in CI, remove
that setting and update the regression test for the temporary skip.

## Dependency maintenance

Direct runtime requirements are limited to DuckDB and Splink. SQLGlot remains a
transitive Splink dependency; development, documentation, training and security
tools stay in separate dependency groups rather than runtime requirements.

Dependabot checks the Python lockfile, frontend npm project and pinned GitHub
Actions at 09:00 UTC on the first and fifteenth of each month. Version updates have
a seven-day cooldown. Python updates include indirect dependencies in `uv.lock`.
Dependabot security updates are enabled separately in GitHub settings and are not
delayed by the version-update schedule or cooldown.

## Exception review

Fix a genuine issue rather than suppress it. For a justified exception, create a
separate maintainer-owned PR, record its reason, scope and evidence, and obtain fresh
security-owner approval. Do not auto-regenerate or wholesale accept scan output. Do
not add broad path, rule, severity, or vulnerability exclusions just to obtain green CI.

The initial secrets baseline contains only these reviewed false positives:

- Two persisted-run identifiers in `benchmarking/compare_runs.py`.
- Three SHA-256 file fingerprints in the canonical test manifest.

The Bandit baseline intentionally contains no accepted findings. Existing dynamic SQL
and other legacy findings require individual maintainer review before the full scan can
pass. Bandit's baseline matching can be broader than an individual source statement;
do not treat a file-level baseline entry as proof that future SQL changes are safe.

All workflows default to `permissions: {}` and grant permissions per job. Read-only
build and test jobs do not inherit publishing or comment permissions. Reusable-workflow
callers explicitly grant the read permission their called jobs require.

Remote actions use full commit SHAs; local reusable workflows use the calling commit.
Shared GitHub Actions caches are disabled, including uv caching and Node's automatic
package-manager cache. Ephemeral dependency downloads are not restored from another run.
Neither `pull_request_target` nor `workflow_run` is permitted. `zizmor.yml` has no
dangerous-trigger exceptions; regression tests check every workflow for these controls.

The benchmark reminder and edge-case results appear in job summaries. Edge-case
comment publication is an explicit maintainer action on `main`, not an automatic
privileged follow-up to contributor code. To publish an existing completed PR run:

```bash
gh workflow run edge_cases_publish_comment.yml --ref main -f run_id=WORKFLOW_RUN_ID
```

The publisher validates the originating workflow, repository, associated open PR,
current SHA, artefact ID, size and regular-file status. It checks out no code and
treats the artefact solely as Markdown. Ambiguous or stale results are not published.

`zizmor` findings of medium-or-higher severity still block CI. Informational and
low-severity style findings remain available through a scan without `--min-severity`.

## GitHub setup and bootstrap

Merge controls through a reviewed maintainer PR before requiring new checks. The
policy job checks out the immutable base SHA and runs only standard-library policy
code, with read-only permissions. Its workflow definition is loaded from the PR,
however, so it is not an independent, immutable security boundary. Keep required team
reviews and protected workflow ownership; use organisation-managed required workflows
for enforcement that a PR cannot edit. Configure branch protection or a ruleset for
`main`:

1. Require PRs, code-owner approval, dismissal of stale approvals, and approval of the
   latest push by someone other than the pusher.
2. Require `Maintainer-only security controls`, `Security and workflow audit`, and
   `JavaScript dependency audit`, binding them to GitHub Actions where supported.
3. Require up-to-date branches, restrict bypass rights, enforce rules for administrators,
   and disallow force-pushes and deletion. Protect release tags separately.
4. For enforcement beyond repository-controlled workflows, use an organisation-managed
   required workflow from a separately protected repository. A local workflow or
   `CODEOWNERS` file alone cannot prevent a privileged administrator disabling controls.

The policy rejects protected-file changes unless both the PR author and triggering
actor have administrator or maintainer roles, with one narrow Dependabot exception:
an official bot-authored PR from this repository's `dependabot/github_actions/`
branches may update only full 40-character action SHAs and version comments in
existing workflow files. Action names, indentation, step order and all other lines
must remain unchanged. The complete diff must be available and match GitHub's change
counts. Bot identity is checked by login, numeric account ID and account type;
ordinary users and other bots receive no exemption. Code-owner approval and the
other required checks still apply before merging these updates.

The policy also checks renamed paths and new inline suppressions, rejects incomplete
source diffs, paginates the GitHub file list, and fails closed on API failures,
oversized PRs, or a changing head SHA. Protected paths include manifests and lockfiles
because these can change scanner tools and execution. Maintainers must recreate
other approved Dependabot proposals, including manifest and lockfile updates, as
maintainer-owned PRs. Dependabot cannot change permissions, commands, baselines or
the policy implementation through the exception.

Confirm `moj-analytical-services/data_linking` exists, is visible to this repository,
has write access for valid code ownership, and has an appropriate maintain/admin
member to manage controls. Enable private vulnerability reporting and Dependabot
alerts/security updates. Keep secret scanning and push protection enabled; review and
restrict delegated bypass rights. Code-owner approval does not prohibit proposed edits.

On 2026-10-14, the repository had code-owner review and stale-approval dismissal enabled,
but no rulesets; administrators could bypass protection and force-pushes were allowed.
Secret scanning and push protection were enabled; Dependabot security updates were not.
Repository settings are not changed by committing these files.

In Actions settings, require full commit-SHA references, keep the default token
read-only with PR-approval permission disabled, and require workflow approval for
all external contributors. These live settings must be checked separately from
workflow YAML. Full SHA pinning covers direct references, not an action's transitive
dependencies or the correctness of the referenced commit.

## Private labels and publishing

Create the `private-labels` environment with security-owner approval, prevent self-review,
and allow deployment only from `main`. Store a narrowly scoped, read-only `LABELS_PAT`
there, then remove the repository-scoped copy. Do not expose its value in a PR or log.
The private-labels workflow runs only trusted `main` code, not contributor PRs. Conduct
any necessary pre-merge private-data evaluation separately on reviewed code in a
controlled environment; approval alone does not make arbitrary PR code trustworthy.

Before the next release, configure a PyPI Trusted Publisher for:

- Owner: `moj-analytical-services`.
- Repository: `uk_address_matcher`.
- Workflow: `pypi-release.yaml`.
- Environment: `release`.

Keep the existing release environment approval and delay. Publishing uses OIDC, with
no API-token fallback. After verifying a trusted release, revoke and remove the old
`PYPI_API_TOKEN`. Release builds disable shared caches and require all security checks.

Use the [two-stage release process](../docs/releasing.md): prepare a mutable draft and
validate its exact source SHA, then manually publish the selected verified artefacts.
The publisher runs on trusted `main` or `build/*` branches; permit both in the protected
environment without removing approval or delay. GitHub publication happens after PyPI and asset
verification, with native immutability retained. There is no automatic destructive
cleanup. The guide documents tag-creation permissions, recovery and sandbox acceptance.

## Privacy and incidents

Review fixture provenance and generated artefacts manually; scanners do not establish
that address data is safe to publish. For exposed credentials, rotate/revoke first,
review affected access, remove exposed artefacts, and coordinate any history cleanup.
For sensitive address data, restrict exposure promptly and follow the organisation's
incident-reporting process. Never copy secret values into a baseline or incident PR.