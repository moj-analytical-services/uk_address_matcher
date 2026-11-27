#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  --dry-run) dry_run=true ;;
  "") dry_run=false ;;
  *) printf 'Usage: bash dev_scripts/shell/prepare-release.sh [--dry-run]\n' >&2; exit 2 ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo=moj-analytical-services/uk_address_matcher
default_branch="$(git -C "$script_dir/../.." branch --show-current)"
read -r -p 'Release tag: ' tag
git check-ref-format "refs/tags/$tag"
read -r -p "Source branch [$default_branch]: " branch
branch="${branch:-$default_branch}"

case "$branch" in
  main|build/*) ;;
  *) printf 'Choose main or a trusted build branch.\n' >&2; exit 1 ;;
esac
if [[ "$dry_run" == false ]]; then
  gh auth status
fi

read -r -p 'Pre-release? [yes/no]: ' prerelease
case "$prerelease" in
  yes|no) ;;
  *) printf 'Enter yes or no for Pre-release.\n' >&2; exit 1 ;;
esac

if [[ "$dry_run" == true ]]; then
  sha="$(git -C "$script_dir/../.." rev-parse --verify "refs/remotes/origin/$branch^{commit}")"
  printf 'Dry run: no GitHub requests; source SHA uses the cached remote branch.\n'
else
  sha="$(gh api "repos/$repo/commits/$branch" --jq .sha)"
fi

create=(gh release create "$tag" --repo "$repo" --draft
  --target "$sha" --generate-notes)
if [[ "$prerelease" == yes ]]; then
  create+=(--prerelease)
fi

if [[ "$dry_run" == true ]]; then
  printf '%q ' "${create[@]}"; printf '\n'
  exit 0
fi

read -r -p 'Create the draft release on GitHub? [yes/no]: ' confirm
if [[ "$confirm" != yes ]]; then
  printf 'Cancelled; nothing created.\n'
  exit 0
fi
"${create[@]}"
printf 'Draft %s created for %s. Review CI results, then publish it manually when ready.\n' "$tag" "$sha"