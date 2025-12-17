#!/bin/bash
# Test the PyPI release workflow locally before deploying
# This mirrors the steps in .github/workflows/pypi-release.yaml

set -e  # Exit on any error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_ENV_DIR="$PROJECT_ROOT/.test-release-env"

echo "=============================================="
echo "Testing PyPI release locally"
echo "Project root: $PROJECT_ROOT"
echo "=============================================="

cd "$PROJECT_ROOT"

# Clean up any previous test environment
if [ -d "$TEST_ENV_DIR" ]; then
    echo "Removing previous test environment..."
    rm -rf "$TEST_ENV_DIR"
fi

# Clean up previous builds
if [ -d "$PROJECT_ROOT/dist" ]; then
    echo "Removing previous build artifacts..."
    rm -rf "$PROJECT_ROOT/dist"
fi

# Step 1: Build the package
echo ""
echo ">>> Step 1: Building package..."
uv build --no-sources

# Check the wheel was created
WHEEL_FILE=$(ls "$PROJECT_ROOT/dist"/*.whl 2>/dev/null | head -n 1)
if [ -z "$WHEEL_FILE" ]; then
    echo "ERROR: No wheel file found in dist/"
    exit 1
fi
echo "Built: $WHEEL_FILE"

# Step 2: Create isolated test environment and install the wheel
echo ""
echo ">>> Step 2: Creating isolated test environment and installing wheel..."
uv venv "$TEST_ENV_DIR"
source "$TEST_ENV_DIR/bin/activate"
uv pip install "$WHEEL_FILE"
uv pip install ipython pandas

# Step 3: Run the example matching script
echo ""
echo ">>> Step 3: Running example_matching.py with TEST_LIMIT=50..."
export TEST_LIMIT=50
python "$PROJECT_ROOT/examples/example_matching.py"

# Clean up
echo ""
echo ">>> Cleaning up test environment..."
deactivate 2>/dev/null || true
rm -rf "$TEST_ENV_DIR"

echo ""
echo "=============================================="
echo "SUCCESS: Release validation passed!"
echo "The package builds and runs correctly."
echo "=============================================="
