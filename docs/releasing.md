# Releasing UK Address Matcher

An ordinary code push does **not** start a release. Both release stages run on the
selected `main` or trusted `build/*` branch; no merge into `main` is required.

```text
Push code and release tooling to the selected trusted branch
   |
   +--> No release starts
   |
   +--> Run dev_scripts/shell/prepare-release.sh
             |
             v
          Create draft + commit candidate request to remote branch
             |
             v
          Validate pinned SHA: Python, package, labelling, security
             |
             +--> Failure: draft retained; nothing published
             |
             v
          All checks pass: retain verified artefacts
             |
             v
          Maintainer creates tag at the verified source SHA
             |
             v
          Run request-publish with successful candidate run ID
             |
             v
          Commit publish request to remote branch; verify artefacts
             |
             v
          Reviewer approval + 60-minute environment wait
             |
             v
          Publish original files to PyPI
             |
             v
          Verify GitHub assets; publish immutable GitHub release last
```

## Before starting

- Install `git`, `gh` and `uv`; authenticate with `gh auth login` as a maintainer.
   Do not use an Actions `GITHUB_TOKEN`: its pushes do not trigger these workflows.
- Set a fresh package version in `pyproject.toml`, then commit and push it with the
   release tooling. `v2.0.0.dev1` is permanently reserved, even after deletion.
- Keep PyPI Trusted Publishing configured for `pypi-release.yaml` and environment
   `release`. Retain reviewer approval, prevention of self-review, the 60-minute wait
   and permission for the chosen branch. Review build-branch release code as carefully
   as `main`.
- Ruleset `25231059` blocks workflow-token tag creation. A maintainer with its existing
   bypass permission must create the verified tag before requesting publication.
   Keep tag-update/deletion protection and native release immutability. Verify the
   full flow with TestPyPI first; local tests do not prove platform permissions.

## Prepare and validate

From the trusted checkout, run:

```bash
bash dev_scripts/shell/prepare-release.sh
```

Enter the package version, source branch (default: current branch) and prerelease
designation (default: `no`). For `2.0.0.dev2`, choose `yes`; for `2.0.0`, choose `no`.

The script pins the remote source SHA, creates a draft and commits
`.github/release-candidate.json` to that remote branch, triggering validation.
Draft creation alone does not trigger it. Your local working tree is unchanged.

In [Actions](https://github.com/moj-analytical-services/uk_address_matcher/actions),
wait for **Release candidate verified** to succeed, then copy the run ID from the
URL. All four checks must pass. Verified artefacts are retained for 30 days.

## Publish

After all checks pass, create the tag at the draft's verified source SHA using your
maintainer account. This creates only the tag; the release must remain a draft:

```bash
TAG=v2.0.0.dev2
REPO=moj-analytical-services/uk_address_matcher
SHA=$(gh release view "$TAG" --repo "$REPO" --json targetCommitish --jq .targetCommitish)
gh api --method POST "repos/$REPO/git/refs" -f "ref=refs/tags/$TAG" -f "sha=$SHA"
```

If tag creation is denied, stop and ask a maintainer with bypass permission. Do not
request publication or weaken the rules. Never recreate or move an existing tag.

Replace the example version and `REPLACE_WITH_RUN_ID` with your verified candidate:

```bash
uv run --no-project --python 3.12 --with packaging==25.0 \
   python -I scripts/release.py request-publish \
   --branch build/ukam_2.0 --tag v2.0.0.dev2 --run-id REPLACE_WITH_RUN_ID
```

This commits `.github/release-publish.json` to the remote branch, starting
**Publish draft release to PyPI**. Review the draft and source SHA in Actions, then
approve the `release` environment. After approval and the wait, the workflow rechecks
and publishes the original artefacts without rebuilding. GitHub publication is last.

**Do not click GitHub's Publish release button directly.** Confirm the PyPI files
and GitHub release afterwards. When your working tree permits, fast-forward your
local branch to include the remote request commits. The tag identifies the tested
source SHA, not the request commits.

## If something fails

- **Request commit fails:** the draft remains; use the retry command printed by the
   script. Do not run preparation again against the existing draft.
- **Validation fails:** fix the failure before publishing. Retry infrastructure
   failures against the same SHA; source changes require a newly pinned candidate.
- **Publishing fails:** use **Re-run failed jobs** on the same publishing run.
   Matching PyPI uploads are reused; never rebuild, delete the draft or move a tag
   as a rollback. PyPI uploads cannot be undone.
- **Artefacts expire:** stop and arrange recovery of the original files. Do not
   assume a rebuild is identical.