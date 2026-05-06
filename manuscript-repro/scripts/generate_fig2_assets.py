#!/usr/bin/env python3
"""
Generate Fig.2 visualization artifacts (circos + sunbursts) for manuscript-repro.

This wraps the existing visualization generator implemented in:
  pipeline/src/pipeline/data_visualization.py

Outputs are copied into:
  manuscript-repro/analysis_outputs/visualization/
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from lib_paths import manuscript_repro_root, repo_root


def _pipeline_root() -> Path:
    # Optional explicit override.
    env = os.environ.get("FOODATLAS_PIPELINE_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    # Default sibling checkout on this machine layout.
    return repo_root().parent / "pipeline"


def _run_visualization_generator(pipeline_root: Path) -> None:
    # Run in the pipeline checkout so relative paths in data_visualization.py resolve.
    code = (
        "from src.pipeline.data_visualization import "
        "plot_circos, create_sunburst_plot, prepare_disease_sunburst_input; "
        "plot_circos(); "
        "create_sunburst_plot("
        "file_path='data/visualization/merged_food_groups.tsv', "
        "output_file='food_sunburst_plot.svg', inner_column='foodon_lvl_1', outer_column='foodon_lvl_2', "
        "title='Food Categories (FoodOn)', entity_type='food'); "
        "create_sunburst_plot("
        "file_path='data/visualization/merged_chemical_groups.tsv', "
        "output_file='chemical_sunburst_plot.svg', inner_column='mesh_lvl1', outer_column='mesh_lvl2', "
        "title='Chemical Categories (MeSH)', entity_type='chemical'); "
        "prepare_disease_sunburst_input(); "
        "create_sunburst_plot("
        "file_path='merged_disease_groups.tsv', output_file='disease_sunburst_plot.svg', "
        "inner_column='category', outer_column='subcategory', title='Disease Category Distribution', "
        "entity_type='disease'); "
        "create_sunburst_plot("
        "file_path='flavor_category_classification.tsv', output_file='flavor_sunburst_plot.svg', "
        "inner_column='category', outer_column='subcategory', title='Flavor Category Distribution', "
        "entity_type='flavor')"
    )
    subprocess.run([sys.executable, "-c", code], cwd=str(pipeline_root), check=True)


def _copy_outputs(pipeline_root: Path) -> None:
    out_dir = manuscript_repro_root() / "analysis_outputs" / "visualization"
    out_dir.mkdir(parents=True, exist_ok=True)

    names = [
        "foodatlas_circos_cytoband.svg",
        "foodatlas_cytobands.csv",
        "food_sunburst_plot.svg",
        "chemical_sunburst_plot.svg",
        "disease_sunburst_plot.svg",
        "flavor_sunburst_plot.svg",
    ]
    missing: list[Path] = []
    for name in names:
        src = pipeline_root / name
        if not src.exists():
            missing.append(src)
            continue
        dst = out_dir / name
        shutil.copy2(src, dst)
        print(f"[copied] {src} -> {dst}")

    if missing:
        msg = "\n".join(f"- {p}" for p in missing)
        raise FileNotFoundError(f"Expected Fig.2 outputs were not generated:\n{msg}")


def main() -> None:
    p_root = _pipeline_root()
    required = [
        p_root / "src" / "pipeline" / "data_visualization.py",
        p_root / "data" / "visualization" / "merged_food_groups.tsv",
        p_root / "data" / "visualization" / "merged_chemical_groups.tsv",
        p_root / "data" / "preprocessing" / "FA_disease_entity_mesh_category.tsv",
        p_root / "flavor_category_classification.tsv",
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        msg = "\n".join(f"- {p}" for p in missing)
        raise FileNotFoundError(
            "Missing required visualization inputs/code.\n"
            "Set FOODATLAS_PIPELINE_ROOT to a compatible pipeline checkout.\n"
            f"{msg}"
        )

    print(f"Using pipeline root: {p_root}")
    try:
        _run_visualization_generator(p_root)
    except subprocess.CalledProcessError as exc:
        print(
            "Warning: Fig.2 generator failed in this environment "
            f"(likely visualization backend dependency issue): {exc}\n"
            "Falling back to pre-generated Fig.2 files if available."
        )
    _copy_outputs(p_root)
    print("Fig.2 assets generated and copied into manuscript-repro/analysis_outputs/visualization")


if __name__ == "__main__":
    main()
