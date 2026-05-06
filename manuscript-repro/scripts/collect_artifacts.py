#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

from lib_paths import (
    PINNED_BIOACTIVITY_RUN,
    PINNED_DISEASE_RUN,
    analysis_artifacts_root,
    manuscript_repro_root,
)


ROOT = manuscript_repro_root()
ANALYSIS = analysis_artifacts_root()
ART = ROOT / "artifacts"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        ensure_dir(dst.parent)
        shutil.copy2(src, dst)
        print(f"[copied] {src} -> {dst}")
    else:
        print(f"[missing] {src}")


def latest_match(base: Path, pattern: str) -> Path | None:
    matches = list(base.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def main() -> None:
    ensure_dir(ART)
    print(f"Using analysis artifact root: {ANALYSIS}")

    # Fig 2 — prefer visualization/ subtree (mirrors old viz bundle output names)
    fig2_dir = ART / "fig2"
    vdir = ANALYSIS / "visualization"
    # Always include local manuscript-repro visualization assets as fallback,
    # even when ANALYSIS points to an external analysis tree.
    local_vdir = ROOT / "analysis_outputs" / "visualization"
    fig2_candidates = {
        "fig2_circos.svg": [
            vdir / "outputs" / "foodatlas_circos_cytoband.svg",
            vdir / "foodatlas_circos_cytoband.svg",
            ANALYSIS / "foodatlas_circos_cytoband.svg",
            local_vdir / "outputs" / "foodatlas_circos_cytoband.svg",
            local_vdir / "foodatlas_circos_cytoband.svg",
        ],
        "fig2_food_sunburst.svg": [
            vdir / "outputs" / "food_sunburst_plot.svg",
            vdir / "food_sunburst_plot.svg",
            local_vdir / "outputs" / "food_sunburst_plot.svg",
            local_vdir / "food_sunburst_plot.svg",
        ],
        "fig2_chemical_sunburst.svg": [
            vdir / "outputs" / "chemical_sunburst_plot.svg",
            vdir / "chemical_sunburst_plot.svg",
            local_vdir / "outputs" / "chemical_sunburst_plot.svg",
            local_vdir / "chemical_sunburst_plot.svg",
        ],
        "fig2_disease_sunburst.svg": [
            vdir / "outputs" / "disease_sunburst_plot.svg",
            vdir / "disease_sunburst_plot.svg",
            local_vdir / "outputs" / "disease_sunburst_plot.svg",
            local_vdir / "disease_sunburst_plot.svg",
        ],
        "fig2_flavor_sunburst.svg": [
            vdir / "outputs" / "flavor_sunburst_plot.svg",
            vdir / "flavor_sunburst_plot.svg",
            local_vdir / "outputs" / "flavor_sunburst_plot.svg",
            local_vdir / "flavor_sunburst_plot.svg",
        ],
        "foodatlas_cytobands.csv": [
            vdir / "outputs" / "foodatlas_cytobands.csv",
            vdir / "foodatlas_cytobands.csv",
            local_vdir / "outputs" / "foodatlas_cytobands.csv",
            local_vdir / "foodatlas_cytobands.csv",
        ],
    }
    for out_name, candidates in fig2_candidates.items():
        src = next((p for p in candidates if p.exists()), None)
        if src is None:
            print(f"[missing] none of: {', '.join(str(p) for p in candidates)}")
            continue
        copy_if_exists(src, fig2_dir / out_name)

    ca = ANALYSIS / "cluster_analysis"

    # Fig 3 / Table 1
    fig3_dir = ART / "fig3_table1"
    disease_viz = ca / "visualizations" / "disease" / PINNED_DISEASE_RUN
    latest_disease_main = latest_match(disease_viz, "tsne_*_disease_main.svg")
    if latest_disease_main:
        copy_if_exists(latest_disease_main, fig3_dir / "fig3_disease_main.svg")

    disease_inter = ca / "intermediate" / "disease" / PINNED_DISEASE_RUN
    copy_if_exists(disease_inter / "portfolio_report.txt", fig3_dir / "table1_portfolio_report.txt")

    latest_risk = latest_match(disease_inter, "tsne_*_risk_features_report.txt")
    if latest_risk:
        copy_if_exists(latest_risk, fig3_dir / "table1_risk_features_report.txt")

    # Fig 4
    fig4_dir = ART / "fig4"
    bio_viz = ca / "visualizations" / "bioactivity" / PINNED_BIOACTIVITY_RUN
    latest_bioactivity = latest_match(bio_viz, "tsne_*_bioactivity.svg")
    if latest_bioactivity:
        copy_if_exists(latest_bioactivity, fig4_dir / "fig4_bioactivity_tsne.svg")
    copy_if_exists(
        ca / "final" / "bioactivity" / "evaluation_curves.svg",
        fig4_dir / "fig4_evaluation_curves.svg",
    )

    # Fig 5 / Table 2–3
    fig5_dir = ART / "fig5_table2_table3"
    vis = ANALYSIS / "substitutions" / "visualizations"
    copy_if_exists(vis / "boxplots.svg", fig5_dir / "fig5_boxplots.svg")
    copy_if_exists(vis / "spider_lollipop.svg", fig5_dir / "fig5_spider_lollipop.svg")
    copy_if_exists(vis / "before_after_heatmap_breakfast.svg", fig5_dir / "fig5_heatmap_breakfast.svg")
    copy_if_exists(vis / "before_after_heatmap_lunch.svg", fig5_dir / "fig5_heatmap_lunch.svg")
    copy_if_exists(vis / "before_after_heatmap_dinner.svg", fig5_dir / "fig5_heatmap_dinner.svg")
    copy_if_exists(vis / "aggregated_sankey_breakfast.svg", fig5_dir / "fig5_sankey_breakfast.svg")
    copy_if_exists(vis / "aggregated_sankey_lunch.svg", fig5_dir / "fig5_sankey_lunch.svg")
    copy_if_exists(vis / "aggregated_sankey_dinner.svg", fig5_dir / "fig5_sankey_dinner.svg")

    sub_final = ANALYSIS / "substitutions" / "final"
    copy_if_exists(sub_final / "disease_example_substitutions_table.csv", fig5_dir / "table2_disease_substitutions.csv")
    copy_if_exists(sub_final / "bioactivity_example_substitutions_table.csv", fig5_dir / "table3_bioactivity_substitutions.csv")

    print("Artifact collection complete.")


if __name__ == "__main__":
    main()
