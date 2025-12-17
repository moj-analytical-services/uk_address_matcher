#!/bin/bash
#
# Bump package version and create a GitHub release.
#
# Usage:
#   chmod +x ./shell/bump_release_version.sh
#   ./shell/bump_release_version.sh <new_version> [--prerelease]
#
# Examples:
#   ./shell/bump_release_version.sh 1.0.0
#   ./shell/bump_release_version.sh 1.0.1.dev1 --prerelease
#
# Requirements:
#   - GitHub CLI (gh) must be installed and authenticated
#   - sed (BSD or GNU)
#
# Note on branch protection:
#   If your repository has branch protection rules preventing direct pushes to
#   main, this script will update the version files locally and commit them,
#   but the push will fail. In that case:
#     1. Create a feature branch with the version bump commit
#     2. Open a PR and merge it
#     3. After merging, run: gh release create v<version> --generate-notes

set -euo pipefail

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Colour

usage() {
    echo "Usage: $0 <new_version> [--prerelease]"
    echo ""
    echo "Arguments:"
    echo "  new_version   The new version string (e.g., 1.0.0 or 1.0.1.dev1)"
    echo "  --prerelease  Mark the GitHub release as a pre-release"
    echo ""
    echo "Examples:"
    echo "  $0 1.0.0"
    echo "  $0 1.0.1.dev1 --prerelease"
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
if [[ $# -lt 1 ]]; then
    usage
fi

# Handle --help flag
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    usage
fi

NEW_VERSION="$1"
PRERELEASE=false

if [[ $# -ge 2 && "$2" == "--prerelease" ]]; then
    PRERELEASE=true
fi

# Validate version format (basic check)
if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(\..+)?$ ]]; then
    log_error "Invalid version format: $NEW_VERSION"
    log_error "Expected format: X.Y.Z or X.Y.Z.suffix (e.g., 1.0.0 or 1.0.0.dev1)"
    exit 1
fi

# Find repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

INIT_FILE="$REPO_ROOT/uk_address_matcher/__init__.py"
PYPROJECT_FILE="$REPO_ROOT/pyproject.toml"

# Verify files exist
if [[ ! -f "$INIT_FILE" ]]; then
    log_error "Could not find $INIT_FILE"
    exit 1
fi

if [[ ! -f "$PYPROJECT_FILE" ]]; then
    log_error "Could not find $PYPROJECT_FILE"
    exit 1
fi

# Check we are on the main branch
CURRENT_BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    log_error "You must be on the 'main' branch to create a release"
    log_error "Current branch: $CURRENT_BRANCH"
    log_info "Switch to main with: git checkout main"
    exit 1
fi

# Check for uncommitted changes
if ! git -C "$REPO_ROOT" diff --quiet || ! git -C "$REPO_ROOT" diff --cached --quiet; then
    log_error "You have uncommitted changes"
    log_error "Please commit or stash your changes before creating a release"
    exit 1
fi

# Check that local main is up to date with remote
log_info "Fetching latest changes from remote..."
if ! git -C "$REPO_ROOT" fetch origin --quiet 2>/dev/null; then
    log_warn "Could not fetch from origin - skipping remote sync check"
    log_warn "Make sure your local main branch is up to date before proceeding"
else
    LOCAL_COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD)
    # Use ls-remote to get the actual remote HEAD for main, avoiding worktree issues
    REMOTE_COMMIT=$(git -C "$REPO_ROOT" ls-remote origin refs/heads/main 2>/dev/null | cut -f1 || echo "")
    if [[ -n "$REMOTE_COMMIT" && "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]]; then
        log_error "Your local main branch is not up to date with origin/main"
        log_error "Local:  $LOCAL_COMMIT"
        log_error "Remote: $REMOTE_COMMIT"
        log_info "Run 'git pull origin main' to update"
        exit 1
    fi
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

# Commit changes
log_info "Committing version bump..."
git -C "$REPO_ROOT" add "$INIT_FILE" "$PYPROJECT_FILE"
git -C "$REPO_ROOT" commit -m "chore: bump version to $NEW_VERSION"

# Check if gh CLI is available
if ! command -v gh &> /dev/null; then
    log_warn "GitHub CLI (gh) is not installed"
    log_warn "Skipping GitHub release creation"
    log_info "To create the release manually, push and run:"
    echo "  git push origin main"
    echo "  # Then create the release via GitHub web UI"
    exit 0
fi

# Check if gh is authenticated
if ! gh auth status &> /dev/null; then
    log_warn "GitHub CLI is not authenticated"
    log_warn "Run 'gh auth login' to authenticate"
    log_warn "Skipping GitHub release creation"
    log_info "Don't forget to push your changes: git push origin main"
    exit 0
fi

# Ask user if they want to create a GitHub release
echo ""
read -p "Do you want to push and create a GitHub release? (y/N) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "Skipping GitHub release creation"
    log_info "Don't forget to push your changes:"
    echo "  git push origin main"
    exit 0
fi

# Push changes (this might fail due to branch protection)
log_info "Pushing changes to remote..."
if ! git -C "$REPO_ROOT" push; then
    log_error "Failed to push changes directly (likely due to branch protection)"
    log_warn "Your version bump has been committed locally but not pushed."
    log_info ""
    log_info "To complete the release with branch protection:"
    echo "  1. Create a feature branch: git checkout -b release/v$NEW_VERSION"
    echo "  2. Push the branch: git push -u origin release/v$NEW_VERSION"
    echo "  3. Open a PR and merge it to main"
    echo "  4. After merging, create the release:"
    echo "     gh release create v$NEW_VERSION --target main --generate-notes"
    if [[ "$PRERELEASE" == true ]]; then
        echo "     (add --prerelease flag for pre-release)"
    fi
    exit 1
fi

# Create GitHub release
TAG_NAME="v$NEW_VERSION"
log_info "Creating GitHub release: $TAG_NAME"

RELEASE_OPTS=(
    "$TAG_NAME"
    --title "$TAG_NAME"
    --generate-notes
)

if [[ "$PRERELEASE" == true ]]; then
    RELEASE_OPTS+=(--prerelease)
    log_info "Marking as pre-release"
fi

if gh release create "${RELEASE_OPTS[@]}"; then
    log_info "GitHub release created successfully: $TAG_NAME"
    log_info "View at: $(gh release view "$TAG_NAME" --json url -q .url)"
else
    log_error "Failed to create GitHub release"
    log_warn "This may be due to branch protection rules"
    log_info "You can create the release manually via the GitHub web interface"
    log_info "Or try running: gh release create $TAG_NAME --title '$TAG_NAME' --generate-notes"
    exit 1
fi

log_info "Version bump complete!"
