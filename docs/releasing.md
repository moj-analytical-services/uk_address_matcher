# Releasing UK Address Matcher

Publishing a release is a manual action in GitHub. Pushes to `main`, `build/*`
and `dev/*` run CI, including the package build, but do not publish to PyPI.

```text
Run dev_scripts/shell/prepare-release.sh
   |
   v
Create draft pinned to the remote source commit
   |
   v
Review CI results; maintainer publishes the draft when confident
   |
   v
Publisher downloads the successful package-build artefact for that exact commit
   |
   v
Reviewer approval + 60-minute environment wait
   |
   v
Download the tested package and upload to PyPI without rebuilding
```

## Before starting

- Install `git` and `gh`; authenticate with `gh auth login` as a maintainer.
- Ensure `pypi-release.yaml` is enabled on the repository's default branch.
   Release source branches must be `main` or `build/*`.
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

## Prepare

From the trusted checkout, run:

```bash
bash dev_scripts/shell/prepare-release.sh
```

Enter the release tag, source branch (default: current branch) and `yes` or `no` for
the prerelease designation. Use `v2.0.0.dev2` with `yes`, or `v2.0.0` with `no`.

The script pins the remote source SHA and creates a draft. It does not dispatch a
separate release-validation workflow or publish the release. Your local working
tree is unchanged. Add `--dry-run` to preview without GitHub requests.

Review the normal CI results for the draft's exact source commit. The publisher
uses the `dist` artefact from that commit's successful **Test build package** run;
the artefact is retained for 30 days.

## Publish

When confident in the release, confirm the draft targets the intended commit and
publish it manually in GitHub. Publishing the draft starts the PyPI publisher
automatically; creating or pushing its tag does not publish the package.

The publisher requires a successful **Test build package** run for the exact tag
commit and downloads its package artefact without rebuilding or rerunning the full
CI suite. Review the other CI checks manually before publishing. Approve the
`release` environment when requested.

For an already-published release, maintainers can manually run **Publish release
to PyPI** with its `release_tag` input. The workflow resolves that published tag
to its commit and uses the same exact-commit package-build artefact.

**GitHub publication makes the release and tag immutable before PyPI upload.** A
failed publisher does not undo that publication. Confirm the PyPI files afterwards;
never delete, recreate or move a published tag as a rollback.

## If something fails

- **No package artefact is available:** rerun **Test build package** for the
   exact source commit before publishing.
- **Publishing fails:** use **Re-run failed jobs** on the same publishing run.
   Matching PyPI uploads are reused. PyPI uploads cannot be undone.
- **The package artefact expires:** rerun **Test build package** for the exact
   source commit. The publisher does not rebuild or substitute a package.