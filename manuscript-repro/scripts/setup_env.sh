#!/usr/bin/env bash
set -euo pipefail

# FoodAtlas-KGv2 repo root (parent of manuscript-repro/)
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${REPO_ROOT}/requirements.txt"

echo "Environment setup complete (repo: ${REPO_ROOT})."
