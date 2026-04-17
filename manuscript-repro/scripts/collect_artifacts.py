#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
PIPELINE = PROJECT_ROOT / "pipeline"
VIZ = PROJECT_ROOT / "visualization_bundle"
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

    # Fig 2
    fig2_dir = ART / "fig2"
    # Prefer visualization_bundle outputs, but fall back to pipeline root artifacts.
    fig2_candidates = {
        "fig2_circos.svg": [
            VIZ / "outputs" / "foodatlas_circos_cytoband.svg",
            PIPELINE / "foodatlas_circos_cytoband.svg",
        ],
        "fig2_food_sunburst.svg": [
            VIZ / "outputs" / "food_sunburst_plot.svg",
            PIPELINE / "food_sunburst_plot.svg",
        ],
        "fig2_chemical_sunburst.svg": [
            VIZ / "outputs" / "chemical_sunburst_plot.svg",
            PIPELINE / "chemical_sunburst_plot.svg",
        ],
        "fig2_disease_sunburst.svg": [
            VIZ / "outputs" / "disease_sunburst_plot.svg",
            PIPELINE / "disease_sunburst_plot.svg",
        ],
        "fig2_flavor_sunburst.svg": [
            VIZ / "outputs" / "flavor_sunburst_plot.svg",
            PIPELINE / "flavor_sunburst_plot.svg",
        ],
        "foodatlas_cytobands.csv": [
            VIZ / "outputs" / "foodatlas_cytobands.csv",
            PIPELINE / "foodatlas_cytobands.csv",
        ],
    }
    for out_name, candidates in fig2_candidates.items():
        src = next((p for p in candidates if p.exists()), None)
        if src is None:
            print(f"[missing] none of: {', '.join(str(p) for p in candidates)}")
            continue
        copy_if_exists(src, fig2_dir / out_name)

    # Fig 3 / Table 1 (use latest disease run)
    fig3_dir = ART / "fig3_table1"
    latest_disease_main = latest_match(
        PIPELINE / "output" / "cluster_analysis" / "visualizations",
        "**/disease/tsne_*_disease_main.svg",
    )
    if latest_disease_main:
        copy_if_exists(latest_disease_main, fig3_dir / "fig3_disease_main.svg")
    copy_if_exists(
        PIPELINE / "output" / "cluster_analysis" / "intermediate" / "disease" / "portfolio_report.txt",
        fig3_dir / "table1_portfolio_report.txt",
    )
    latest_risk = latest_match(
        PIPELINE / "output" / "cluster_analysis" / "intermediate" / "disease",
        "tsne_*_risk_features_report.txt",
    )
    if latest_risk:
        copy_if_exists(latest_risk, fig3_dir / "table1_risk_features_report.txt")

    # Fig 4
    fig4_dir = ART / "fig4"
    latest_bioactivity = latest_match(
        PIPELINE / "output" / "cluster_analysis" / "visualizations",
        "**/bioactivity/tsne_*_bioactivity.svg",
    )
    if latest_bioactivity:
        copy_if_exists(latest_bioactivity, fig4_dir / "fig4_bioactivity_tsne.svg")
    copy_if_exists(
        PIPELINE / "output" / "cluster_analysis" / "final" / "bioactivity" / "evaluation_curves.svg",
        fig4_dir / "fig4_evaluation_curves.svg",
    )

    # Fig 5 / Table 2-3
    fig5_dir = ART / "fig5_table2_table3"
    vis = PIPELINE / "output" / "substitutions" / "visualizations"
    copy_if_exists(vis / "boxplots.svg", fig5_dir / "fig5_boxplots.svg")
    copy_if_exists(vis / "spider_lollipop.svg", fig5_dir / "fig5_spider_lollipop.svg")
    copy_if_exists(vis / "before_after_heatmap_breakfast.svg", fig5_dir / "fig5_heatmap_breakfast.svg")
    copy_if_exists(vis / "before_after_heatmap_lunch.svg", fig5_dir / "fig5_heatmap_lunch.svg")
    copy_if_exists(vis / "before_after_heatmap_dinner.svg", fig5_dir / "fig5_heatmap_dinner.svg")
    copy_if_exists(vis / "aggregated_sankey_breakfast.svg", fig5_dir / "fig5_sankey_breakfast.svg")
    copy_if_exists(vis / "aggregated_sankey_lunch.svg", fig5_dir / "fig5_sankey_lunch.svg")
    copy_if_exists(vis / "aggregated_sankey_dinner.svg", fig5_dir / "fig5_sankey_dinner.svg")

    sub_final = PIPELINE / "output" / "substitutions" / "final"
    copy_if_exists(sub_final / "disease_example_substitutions_table.csv", fig5_dir / "table2_disease_substitutions.csv")
    copy_if_exists(sub_final / "bioactivity_example_substitutions_table.csv", fig5_dir / "table3_bioactivity_substitutions.csv")

    print(f"Done. Collected artifacts under {ART}")


if __name__ == "__main__":
    main()
