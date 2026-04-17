#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "${ROOT_DIR}/.." && pwd)"

python3 -m pip install --upgrade pip
python3 -m pip install -r "${PROJECT_ROOT}/pipeline/requirements.txt"
python3 -m pip install -r "${PROJECT_ROOT}/visualization_bundle/requirements.txt"

echo "Environment setup complete."
