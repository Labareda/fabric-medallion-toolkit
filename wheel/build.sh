#!/usr/bin/env bash
# Build the wheel, leaving ONLY the current version in dist/.
#
# `python -m build` appends to dist/ -- it never removes older .whl files.
# Left alone, dist/ accumulates every version ever built (0.3.55, 0.3.56,
# 0.3.57, ...) side by side, and it becomes easy to download and upload the
# WRONG one to Fabric. Clearing dist/ first guarantees the only .whl in the
# folder is the one you just built from the current pyproject.toml version.
#
# Usage (from the wheel/ directory):
#   ./build.sh
set -euo pipefail

cd "$(dirname "$0")"

echo "Clearing old build artifacts..."
rm -rf dist/ build/

echo "Building wheel from pyproject.toml version $(grep '^version' pyproject.toml | cut -d'"' -f2)..."
python -m build

echo
echo "Done. dist/ now contains only:"
ls -1 dist/
