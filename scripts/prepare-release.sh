#!/usr/bin/env bash
set -euo pipefail

for tool in gh uv; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'Install %s before preparing a release.\n' "$tool" >&2
    exit 1
  fi
done

read -r -p 'Version (for example 2.0.0.dev2): ' version
read -r -p 'Source branch [main]: ' branch
read -r -p 'Pre-release? [no]: ' prerelease
branch="${branch:-main}"
prerelease="${prerelease:-no}"
case "$prerelease" in
  yes|no) ;;
  *) printf 'Answer yes or no for Pre-release.\n' >&2; exit 1 ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --no-project --python 3.12 --with packaging==25.0 python -I \
  "$script_dir/release.py" prepare --version "$version" --branch "$branch" \
  --prerelease "$prerelease"