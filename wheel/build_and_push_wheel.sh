#!/usr/bin/env bash
# Build the wheel, verify it, and push the built .whl to git so it can be
# downloaded from wherever you access the client's Fabric workspace.
#
# Run this from the `wheel/` directory (same place you've been running
# `python -m build` from).
#
# Usage:
#   ./build_and_push_wheel.sh
#
# Before running: bump the version in pyproject.toml if this is a real
# code change (not just a rebuild), so Fabric doesn't keep resolving a
# stale cached wheel under the old version number.

set -euo pipefail

echo "=== 1. Clean previous build artifacts ==="
rm -rf dist build src/*.egg-info

echo ""
echo "=== 2. Build ==="
pip install build --quiet
python -m build

WHL=$(ls dist/*.whl)
echo ""
echo "Built: $WHL"

echo ""
echo "=== 3. Verify the build (mirrors the checks we've been doing by hand) ==="
python3 -c "
import zipfile, sys

whl = '$WHL'
z = zipfile.ZipFile(whl)

# Version + dependencies
meta_name = [n for n in z.namelist() if n.endswith('METADATA')][0]
meta = z.read(meta_name).decode()
version_line = [l for l in meta.splitlines() if l.startswith('Version')]
deps = [l for l in meta.splitlines() if l.startswith('Requires-Dist')]
print('Version:', version_line[0] if version_line else 'MISSING')
print('Dependencies:', deps if deps else 'none')

# No junk (pycache, .pyc, dotfiles)
junk = [n for n in z.namelist() if '__pycache__' in n or n.endswith('.pyc') or n.startswith('.')]
if junk:
    print('WARNING - junk files found:', junk)
    sys.exit(1)
else:
    print('Junk check: clean')

print('Total files in wheel:', len(z.namelist()))
"

echo ""
echo "=== 4. Hash (compare this after downloading elsewhere, to rule out transfer corruption) ==="
sha256sum "$WHL"

echo ""
echo "=== 5. Commit and push the built .whl to git ==="
# dist/ is normally gitignored (it's a build artifact) -- force-add just
# this one file so it's available to download from GitHub.
git add -f "$WHL"
git status --short

read -p "Commit and push this wheel to git now? [y/N] " CONFIRM
if [[ "$CONFIRM" =~ ^[Yy]$ ]]; then
    VERSION=$(python3 -c "import re; print(re.search(r'version = \"([^\"]+)\"', open('pyproject.toml').read()).group(1))")
    git commit -m "Build wheel ${VERSION}"
    git push
    echo ""
    echo "Pushed. Download $WHL from GitHub on whichever machine you use to access Fabric,"
    echo "then re-run the sha256sum check above on the downloaded copy to confirm it matches."
else
    echo "Skipped commit/push -- $WHL is built and staged locally if you want to push manually."
fi
