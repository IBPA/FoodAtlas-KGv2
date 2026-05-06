#!/usr/bin/env bash
set -euo pipefail

# FoodAtlas-KGv2 repository root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MR_DIR="${REPO_ROOT}/manuscript-repro"
LOG_DIR="${MR_DIR}/logs"
mkdir -p "${LOG_DIR}"

echo "================================================================"
echo "FoodAtlas-KGv2 — manuscript-repro driver"
echo "================================================================"
echo ""
echo "This repo is self-contained: nothing here references directories"
echo "outside the clone (no sibling pipeline/ or visualization_bundle/)."
echo ""
echo "1) Knowledge graph construction"
echo "   Follow the main README and scripts/README.md:"
echo "   - ./scripts/0_run_kg_init.sh"
echo "   - ./scripts/1_run_metadata_processing.sh (configure PATH_INPUT)"
echo "   - ./scripts/2_run_adding_triplets_from_metadata.sh"
echo "   - ./scripts/3_run_postprocessing.sh"
echo "   Outputs land under outputs/kg/"
echo ""

cd "${REPO_ROOT}"

if [[ -f outputs/kg/triplets.tsv ]]; then
  echo "[sanity] Running KG test module on outputs/kg ..."
  python3 -m food_atlas.tests.test_kg outputs/kg 2>&1 | tee "${LOG_DIR}/kg_test.log" || {
    echo "Warning: test_kg reported issues (see ${LOG_DIR}/kg_test.log)."
  }
else
  echo "[skip] No outputs/kg/triplets.tsv yet — add graph files under outputs/kg/ or run the KG stages above."
fi

echo ""
echo "2) Manuscript figures 2–5 (clustering, substitutions, circos)"
echo "   These analyses are not executed by FoodAtlas-KGv2 build scripts."
echo "   To verify or collect those artifacts, populate:"
echo "     ${MR_DIR}/analysis_outputs/"
echo "   with the same layout as the paper analysis pipeline output/"
echo "   (see analysis_outputs/README.md), or set:"
echo "     FOODATLAS_MANUSCRIPT_ANALYSIS_DIR=/path/inside/your/clone"
echo ""
echo "Done. Logs: ${LOG_DIR}"
