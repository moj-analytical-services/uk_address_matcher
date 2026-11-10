# Releasing UK Address Matcher

Prepare a mutable draft and validate it first. Publish only through the protected
publishing workflow. **Do not click GitHub's Publish release button directly.**

## Before enabling this process

Merge the release scripts and workflows onto trusted `main`. Candidate workflow
definitions run there; publishing definitions may run on `main` or trusted `build/*`
branches. The candidate's source code can come from another explicitly selected
remote branch. All four checks receive its pinned SHA.
Changes to the release scripts and their tests require the existing security review.

Configure the `release` environment to permit **branch `main`** and **branches `build/*`**,
retaining reviewer approval, prevention of self-review and the 60-minute wait. Keep PyPI's Trusted
Publisher registered for `pypi-release.yaml` and environment `release`. No PyPI API
token is used. Keep native GitHub release immutability enabled.

Ruleset `25231059` previously blocked tag creation with a team-only bypass. The
workflow token needs to be able to create the final tag when the draft is published;
`contents: write` does not bypass a ruleset. Arrange the narrowest approved creation
policy before live use. Do not remove tag-update/deletion restrictions. No GitHub App
or live setting changes are made by these scripts.

GitHub has no general native branch rule that allows drafts while forbidding an
authorised human from publishing them directly. This automation rejects non-drafts;
the UI restriction remains a maintainer policy, not an enforced platform guarantee.

## Prepare and validate

1. Install GitHub CLI and `uv`, then run `gh auth login`. Use a trusted checkout of
   the scripts and ensure the intended source branch is pushed to GitHub.
2. Set the intended package version in the source branch before preparing it. Stable
   `2.0.0` uses Pre-release **no**; `2.0.0.dev2` uses **yes**. Local versions are rejected.
3. Run:

   ```bash
   ./scripts/prepare-release.sh
   ```

4. Enter the version, source branch (default `main`) and prerelease designation
   (default `no`). The script resolves the branch to an exact SHA, checks version
   collisions, creates a draft targeted at that SHA and dispatches validation on
   trusted `main`. It never moves tags or overwrites drafts. GitHub may show a draft
   at an `untagged-*` URL; the verifier uses its API ID and exact tag name.
5. Open [Release candidate runs](https://github.com/moj-analytical-services/uk_address_matcher/actions/workflows/release-candidate.yml).
   Python, package-build, labelling and security gates run in parallel. The final
   **Release candidate verified** gate requires all four to succeed.
6. Record the successful run ID from the Actions URL. Its manifest records the draft
   ID, source branch/SHA, workflow SHA, attempt, distribution artefact ID, versions,
   gate results and SHA-256 hashes. Distributions are built once and retained for
   30 days. Each attempt retains separate artefacts for recovery.

Draft creation alone never triggers validation. If dispatch fails, the script leaves
the draft and prints the exact command to re-dispatch. For example:

```bash
gh workflow run release-candidate.yml \
  --repo moj-analytical-services/uk_address_matcher --ref main \
  -f release_tag=v2.0.0.dev2 -f source_branch=main \
  -f target_sha=REPLACE_WITH_EXACT_40_CHARACTER_SHA -f prerelease=true
```

## Publish

1. Open [Publish draft release to PyPI](https://github.com/moj-analytical-services/uk_address_matcher/actions/workflows/pypi-release.yaml).
2. Choose **Run workflow**, branch **main** or a trusted **build/** branch, the draft's
   release tag and successful candidate run ID. This workflow does not rebuild or
   rerun the validation DAG. Keep publishing scripts on `build/*` branches under the
   same review controls as `main`: they execute with release authority after approval.
3. Inspection checks the trusted candidate workflow and successful attempt, all four
   gates, draft identity/SHA/prerelease setting and both distribution metadata/hashes.
   Missing, expired or mismatched artefacts stop publication. Existing PyPI files
   must exactly match the original verified files.
4. Review the selected draft and SHA, then approve the `release` environment. The
   approved job rechecks those same immutable artefact IDs after the wait and uploads
   the exact files to PyPI using OIDC.
5. GitHub finalisation downloads those original artefacts again, confirms both files
   are on PyPI, attaches them to the draft, downloads/checks the attached assets and
   only then publishes the draft. It verifies the final tag SHA and immutability.
6. Confirm the PyPI files and published immutable GitHub release.

Only the PyPI job has publishing OIDC permission. Only GitHub finalisation has
repository write permission. Neither executes the candidate source code.

## Recovery

- **Failed validation:** keep the draft. Retry infrastructure failures against the
  same pinned commit. Cancelled or skipped gates cannot authorise publication.
- **Source changes:** do not silently reuse the old successful run. Explicitly update
  the mutable draft's target SHA before dispatching a new candidate. If a backing tag
  already points elsewhere, use a fresh version rather than move it. Preparation
  refuses existing drafts; it does not retarget them automatically.
- **Rejected approval:** nothing was uploaded. Retry publication after approval is
  available, while the verified artefacts are retained.
- **Partial PyPI upload:** files/version numbers cannot be overwritten. Use **Re-run
  failed jobs** on the same publishing run; matching uploaded files are skipped and
  changed files are rejected. Never rebuild or delete the draft/tag as a rollback.
- **Failed GitHub finalisation:** rerun only failed jobs in that publishing run. It
  retains the original manifest ID even if candidate validation is rerun later.
  Draft asset upload is retryable; already-published matching assets are not replaced.
- **Expired artefacts:** stop. Restore the exact original distributions through an
  explicitly reviewed recovery procedure; do not assume a rebuild is identical.
- **Previously immutable version:** deleting a release/tag does not make that name
  reusable. For example `v2.0.0.dev1` is permanently reserved; use a fresh version.
  Normal tag/release lookups do not expose GitHub's deleted-name reservation history.

There is no automatic deletion, tag movement or rollback. Simultaneous publishing
attempts for the same tag do not run concurrently; this is not a guaranteed FIFO queue.

## Integration acceptance

Before enabling live PyPI, test the complete flow in a disposable repository with
TestPyPI and matching protection settings. The helper's repository/PyPI endpoints
and upload URL must be changed only in that sandbox, with its own Trusted Publisher.
Specifically verify draft creation without a backing tag (including prereleases),
ruleset permissions for the final workflow token, deliberate failed/cancelled checks,
approval rejection, partial upload and finalisation retry. Offline tests cannot prove
these platform behaviours. Do not create live releases or relax production settings
as a substitute for this sandbox test.