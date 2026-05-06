"""
Path helpers for manuscript-repro scripts.

All paths are resolved inside the FoodAtlas-KGv2 repository root (parent of
`manuscript-repro/`). No sibling `pipeline/` or `visualization_bundle/` dirs.
"""

from __future__ import annotations

import os
from pathlib import Path


def manuscript_repro_root() -> Path:
    """Directory containing this package (`manuscript-repro/`)."""
    return Path(__file__).resolve().parents[1]


def repo_root() -> Path:
    """FoodAtlas-KGv2 repository root (contains `food_atlas/`, `outputs/`, `scripts/`)."""
    return Path(__file__).resolve().parents[2]


def kg_dir() -> Path:
    """Canonical KG export directory: `<repo>/outputs/kg/` (see repo `outputs/README.md`).

    Override with env ``FOODATLAS_KG_DIR`` (absolute path) for tests or alternate layouts.
    """
    env = os.environ.get("FOODATLAS_KG_DIR")
    if env:
        return Path(env).expanduser().resolve()
    return repo_root() / "outputs" / "kg"


def analysis_artifacts_root() -> Path:
    """
    Optional downstream manuscript analysis outputs (Fig. 2–5 style assets).

    Default: `manuscript-repro/analysis_outputs/` with the same subtree layout as
    the publication analysis pipeline's `output/` directory (`cluster_analysis/`,
    `substitutions/`, optional `visualization/` for circos/sunbursts).

    Override with env `FOODATLAS_MANUSCRIPT_ANALYSIS_DIR` (must be absolute or
    relative to cwd; resolved to an absolute path).
    """
    env = os.environ.get("FOODATLAS_MANUSCRIPT_ANALYSIS_DIR")
    if env:
        return Path(env).expanduser().resolve()
    return manuscript_repro_root() / "analysis_outputs"


# Pinned analysis run ids used for deterministic checks (match publication snapshot).
PINNED_DISEASE_RUN = "run_20251104_non_neonatal_mwu_v1"
PINNED_BIOACTIVITY_RUN = "run_20251104_non_neonatal_mwu_v1"
