#!/usr/bin/env bash
set -euo pipefail

validate_only=false
case "${1:-}" in
  --dry-run) dry_run=true ;;
  --validate-only) dry_run=false; validate_only=true ;;
  "") dry_run=false ;;
  *) printf 'Usage: bash dev_scripts/shell/prepare-release.sh [--dry-run|--validate-only]\n' >&2; exit 2 ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo=moj-analytical-services/uk_address_matcher
default_branch="$(git -C "$script_dir/../.." branch --show-current)"
if [[ "$validate_only" == false ]]; then
  read -r -p 'Release tag: ' tag
  git check-ref-format "refs/tags/$tag"
fi
read -r -p "Source branch [$default_branch]: " branch
branch="${branch:-$default_branch}"

case "$branch" in
  main|build/*) ;;
  *) printf 'Choose main or a trusted build branch.\n' >&2; exit 1 ;;
esac
validate=(gh workflow run release-validation.yml --repo "$repo" --ref "$branch")
if [[ "$dry_run" == false ]]; then
  gh auth status
  if ! gh workflow view release-validation.yml --repo "$repo" --yaml --ref "$branch" >/dev/null; then
    printf 'Validation workflow unavailable. Deploy it on main and the selected branch before retrying.\n' >&2
    exit 1
  fi
fi
if [[ "$validate_only" == true ]]; then
  "${validate[@]}"
  printf 'Validation started; this manual run cannot publish to PyPI.\n'
  exit 0
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
  printf '%q ' "${validate[@]}"; printf '\n'
  exit 0
fi

read -r -p 'Create the draft and start validation on GitHub? [yes/no]: ' confirm
if [[ "$confirm" != yes ]]; then
  printf 'Cancelled; nothing created.\n'
  exit 0
fi
"${create[@]}"
if ! "${validate[@]}"; then
  printf 'Validation did not start. The draft is retained; retry with --validate-only.\n' >&2
  exit 1
fi
printf 'Review the validation results, then publish the draft manually when ready.\n'