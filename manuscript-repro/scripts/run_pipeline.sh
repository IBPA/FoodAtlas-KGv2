#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "${ROOT_DIR}/.." && pwd)"
PIPELINE_DIR="${PROJECT_ROOT}/pipeline"
VIZ_DIR="${PROJECT_ROOT}/visualization_bundle"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

echo "[1/4] Running preprocessing pipeline..."
(
  cd "${PIPELINE_DIR}"
  python3 -c "from src.pipeline.preprocessing import preprocessing_pipeline; preprocessing_pipeline()" | tee "${LOG_DIR}/01_preprocessing.log"
)

echo "[2/4] Running cluster analysis..."
(
  cd "${PIPELINE_DIR}"
  python3 -c "from src.pipeline.cluster_analysis import cluster_analysis; cluster_analysis()" | tee "${LOG_DIR}/02_cluster_analysis.log"
)

echo "[3/4] Running substitution analysis..."
(
  cd "${PIPELINE_DIR}"
  python3 -c "from src.pipeline.substitution_analysis import substitution_analysis; substitution_analysis()" | tee "${LOG_DIR}/03_substitution_analysis.log"
)

echo "[4/4] Running visualization bundle (circos + sunbursts)..."
(
  cd "${VIZ_DIR}"
  python3 run_all.py | tee "${LOG_DIR}/04_visualization_bundle.log"
)

echo "Pipeline complete. Logs are in ${LOG_DIR}"
