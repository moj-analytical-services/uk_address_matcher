#!/usr/bin/env bash
set -euo pipefail

for tool in gh uv git; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'Install %s before preparing a release.\n' "$tool" >&2
    exit 1
  fi
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
default_branch="$(git -C "$script_dir/.." branch --show-current)"
printf 'Creates a draft and commits a validation request to the selected remote branch.\n'
read -r -p 'Version (for example 2.0.0.dev2): ' version
read -r -p "Source branch [$default_branch]: " branch
read -r -p 'Pre-release? [no]: ' prerelease
branch="${branch:-$default_branch}"
prerelease="${prerelease:-no}"
case "$prerelease" in
  yes|no) ;;
  *) printf 'Answer yes or no for Pre-release.\n' >&2; exit 1 ;;
esac

exec uv run --no-project --python 3.12 --with packaging==25.0 python -I \
  "$script_dir/release.py" prepare --version "$version" --branch "$branch" \
  --prerelease "$prerelease"