# Releasing UK Address Matcher

An ordinary code push does **not** start a release. Pushes to `main`, `build/*`
and `dev/*` build the package independently of release validation.

```text
Run dev_scripts/shell/prepare-release.sh
   |
   v
Create draft pinned to the remote source commit
   |
   v
Dispatch validation: Python, package, labelling, security
   |
   +--> Failure: draft retained; nothing published
   |
   v
All four checks pass: retain tested package artefacts
   |
   v
Maintainer publishes the draft in GitHub
   |
   v
Publisher requires successful validation of the exact tagged commit
   |
   v
Reviewer approval + 60-minute environment wait
   |
   v
Download the tested package and upload to PyPI without rebuilding
```

## Before starting

- Install `git` and `gh`; authenticate with `gh auth login` as a maintainer.
- Deploy `release-validation.yml` and `pypi-release.yaml` to `main` and the selected
   release branch first. GitHub must register the validation workflow on the default
   branch before it can be dispatched. Release branches must be `main` or `build/*`.
- Set a fresh package version, then commit and push the source and release tooling.
   `v2.0.0.dev1` is permanently reserved, even after deletion.
- Keep PyPI Trusted Publishing configured for `pypi-release.yaml` and environment
   `release`. Retain reviewer approval, prevention of self-review, the 60-minute wait
   and permission for the chosen branch. Review build-branch release code as carefully
   as `main`.
- Keep tag-update/deletion protection and native release immutability. Publishing
   the draft is a human action; the workflows do not create or move tags.
- Verify the full flow with TestPyPI first; local tests do not prove platform
   permissions or PyPI Trusted Publishing configuration.

## Prepare and validate

From the trusted checkout, run:

```bash
bash dev_scripts/shell/prepare-release.sh
```

Enter the release tag, source branch (default: current branch) and `yes` or `no` for
the prerelease designation. Use `v2.0.0.dev2` with `yes`, or `v2.0.0` with `no`.

The script pins the remote source SHA, creates a draft and dispatches validation
on the selected branch. Draft creation alone does not trigger validation. Your
local working tree is unchanged. Add `--dry-run` to preview without GitHub requests.

In [Actions](https://github.com/moj-analytical-services/uk_address_matcher/actions),
wait for **Validate release** to succeed. All four checks must pass; tested package
artefacts are retained for 30 days. No run ID needs to be copied.

To validate an existing draft's source without creating another draft:

```bash
bash dev_scripts/shell/prepare-release.sh --validate-only
```

## Publish

After all checks pass, confirm that the draft targets the exact validated commit,
then publish the draft manually in GitHub. If the branch advanced after draft
creation, align the draft target and validation before publishing.

The publisher requires the latest validation run for that exact commit to have
succeeded on `main` or a trusted `build/*` branch. It downloads that run's tested
package and uploads it to PyPI without building or running the checks again.
Approve the `release` environment when requested. Version-tag pushes remain a
fallback trigger with the same validation requirement, not a required extra step.

**GitHub publication makes the release and tag immutable before PyPI upload.** A
failed publisher does not undo that publication. Confirm the PyPI files afterwards;
never delete, recreate or move a published tag as a rollback.

## If something fails

- **Validation does not start:** the draft remains; use `--validate-only` after
   resolving workflow registration or dispatch permissions.
- **Validation fails:** fix the failure before publishing. Retry infrastructure
   failures against the same SHA; source changes require an updated draft target
   and successful validation of the new commit.
- **Publishing fails:** use **Re-run failed jobs** on the same publishing run.
   Matching PyPI uploads are reused. PyPI uploads cannot be undone.
- **Artefacts expire:** run fresh validation before publication. The publisher
   cannot rebuild or substitute an untested package.