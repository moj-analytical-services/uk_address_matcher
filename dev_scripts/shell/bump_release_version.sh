#!/bin/bash
#
# Bump package version and push a release branch.
#
# Usage:
#   chmod +x ./dev_scripts/shell/bump_release_version.sh
#   ./dev_scripts/shell/bump_release_version.sh <new_version>
#
# Example:
#   ./dev_scripts/shell/bump_release_version.sh 1.0.0
#
# The script updates the package version and uv.lock, commits the changes to
# release/<version>, and pushes that branch for review. It does not publish.

set -euo pipefail

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Colour

usage() {
    echo "Usage: $0 <new_version>"
    echo ""
    echo "Arguments:"
    echo "  new_version   The new version string (e.g., 1.0.0 or 1.0.1.dev1)"
    echo "Examples:"
    echo "  $0 1.0.0"
    exit 1
}

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check arguments
if [[ $# -ne 1 ]]; then
    usage
fi

# Handle --help flag
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    usage
fi

NEW_VERSION="$1"

# Validate version format (basic check)
if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\..+)?$ ]]; then
    log_error "Invalid version format: $NEW_VERSION"
    log_error "Expected format: X.Y.Z or X.Y.Z.suffix (e.g., 1.0.0 or 1.0.0.dev1)"
    exit 1
fi

# Find repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

INIT_FILE="$REPO_ROOT/uk_address_matcher/__init__.py"
PYPROJECT_FILE="$REPO_ROOT/pyproject.toml"
UV_LOCK_FILE="$REPO_ROOT/uv.lock"
RELEASE_BRANCH="release/$NEW_VERSION"

# Verify files exist
if [[ ! -f "$INIT_FILE" ]]; then
    log_error "Could not find $INIT_FILE"
    exit 1
fi

if [[ ! -f "$PYPROJECT_FILE" ]]; then
    log_error "Could not find $PYPROJECT_FILE"
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    log_error "uv is required to update uv.lock"
    exit 1
fi

# Create the release branch from the current named branch
CURRENT_BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" == "HEAD" ]]; then
    log_error "You must be on a named branch to create a release"
    exit 1
fi

# Check for uncommitted changes
if ! git -C "$REPO_ROOT" diff --quiet || ! git -C "$REPO_ROOT" diff --cached --quiet; then
    log_error "You have uncommitted changes"
    log_error "Please commit or stash your changes before creating a release"
    exit 1
fi

# Fetch remote refs for release branch and tag collision checks
log_info "Fetching remote refs from origin..."
if ! git -C "$REPO_ROOT" fetch origin --quiet 2>/dev/null; then
    log_error "Could not fetch from origin"
    exit 1
fi

# Check that the release branch does not already exist
if git -C "$REPO_ROOT" show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
    log_error "Branch '$RELEASE_BRANCH' already exists locally"
    exit 1
fi

if git -C "$REPO_ROOT" ls-remote --exit-code --heads origin "$RELEASE_BRANCH" >/dev/null 2>&1; then
    log_error "Branch '$RELEASE_BRANCH' already exists on remote"
    exit 1
fi

# Check if the tag already exists (locally or remotely)
TAG_NAME="v$NEW_VERSION"
if git -C "$REPO_ROOT" tag -l "$TAG_NAME" | grep -q "$TAG_NAME"; then
    log_error "Tag '$TAG_NAME' already exists locally"
    log_error "Choose a different version number"
    exit 1
fi

if git -C "$REPO_ROOT" ls-remote --tags origin "$TAG_NAME" | grep -q "$TAG_NAME"; then
    log_error "Tag '$TAG_NAME' already exists on remote"
    log_error "Choose a different version number"
    exit 1
fi

# Extract current versions
CURRENT_INIT_VERSION=$(grep -oE '__version__ = "[^"]+"' "$INIT_FILE" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+[^"]*')
CURRENT_PYPROJECT_VERSION=$(grep -E '^version = "[^"]+"' "$PYPROJECT_FILE" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+[^"]*')

log_info "Current version in __init__.py: $CURRENT_INIT_VERSION"
log_info "Current version in pyproject.toml: $CURRENT_PYPROJECT_VERSION"
log_info "New version: $NEW_VERSION"

# Confirm with user
echo ""
read -p "Do you want to proceed with the version bump? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_warn "Aborted by user"
    exit 0
fi

# Create the release branch from the current branch before changing versions
log_info "Using current branch as release base: $CURRENT_BRANCH"
log_info "Creating release branch: $RELEASE_BRANCH"
git -C "$REPO_ROOT" checkout -b "$RELEASE_BRANCH"

# Bump version in __init__.py
log_info "Updating $INIT_FILE..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS (BSD sed)
    sed -i '' "s/__version__ = \".*\"/__version__ = \"$NEW_VERSION\"/" "$INIT_FILE"
else
    # Linux (GNU sed)
    sed -i "s/__version__ = \".*\"/__version__ = \"$NEW_VERSION\"/" "$INIT_FILE"
fi

# Bump version in pyproject.toml
log_info "Updating $PYPROJECT_FILE..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS (BSD sed)
    sed -i '' "s/^version = \".*\"/version = \"$NEW_VERSION\"/" "$PYPROJECT_FILE"
else
    # Linux (GNU sed)
    sed -i "s/^version = \".*\"/version = \"$NEW_VERSION\"/" "$PYPROJECT_FILE"
fi

# Verify changes
NEW_INIT_VERSION=$(grep -oE '__version__ = "[^"]+"' "$INIT_FILE" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+[^"]*')
NEW_PYPROJECT_VERSION=$(grep -E '^version = "[^"]+"' "$PYPROJECT_FILE" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+[^"]*')

if [[ "$NEW_INIT_VERSION" != "$NEW_VERSION" ]]; then
    log_error "Failed to update __init__.py"
    exit 1
fi

if [[ "$NEW_PYPROJECT_VERSION" != "$NEW_VERSION" ]]; then
    log_error "Failed to update pyproject.toml"
    exit 1
fi

log_info "Version files updated successfully"

# Refresh the project lockfile for the new package version
log_info "Updating uv.lock..."
if ! (cd "$REPO_ROOT" && uv lock); then
    log_error "Failed to update uv.lock"
    exit 1
fi
if [[ ! -f "$UV_LOCK_FILE" ]]; then
    log_error "uv lock completed without creating $UV_LOCK_FILE"
    exit 1
fi

# Commit changes
log_info "Committing version bump..."
git -C "$REPO_ROOT" add "$INIT_FILE" "$PYPROJECT_FILE" "$UV_LOCK_FILE"
git -C "$REPO_ROOT" commit -m "chore: bump version to $NEW_VERSION"

log_info "Pushing release branch..."
if ! git -C "$REPO_ROOT" push --set-upstream origin "$RELEASE_BRANCH"; then
    log_error "Failed to push '$RELEASE_BRANCH'"
    log_warn "The version bump is committed locally on '$RELEASE_BRANCH'."
    log_info "Push it manually with: git -C '$REPO_ROOT' push --set-upstream origin '$RELEASE_BRANCH'"
    exit 1
fi

log_info "Release branch pushed: $RELEASE_BRANCH"
log_info "Open a pull request for review; publication is not performed by this script."
log_info "Version bump complete!"
