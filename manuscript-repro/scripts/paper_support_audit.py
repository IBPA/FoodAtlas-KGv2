#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from manuscript_check import run_checks, PINNED_BIOACTIVITY_RUN, PINNED_DISEASE_RUN


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
PIPELINE = PROJECT_ROOT / "pipeline"
ART = ROOT / "artifacts"


def _index_results(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for r in results:
        out[f"{r['claim']}::{r['metric']}"] = r
    return out


def _status_level(statuses: list[str]) -> str:
    # Claim-level status aggregation:
    # - supported: all match/close and at least one match
    # - partially_supported: any mismatch/missing but claim direction remains
    # - unsupported: all missing or major failures
    if not statuses:
        return "unsupported"
    s = set(statuses)
    if s.issubset({"match", "close"}) and "match" in s:
        return "supported"
    if s.issubset({"missing"}):
        return "unsupported"
    return "partially_supported"


def _exists(p: Path) -> bool:
    return p.exists()


def build_audit() -> dict[str, Any]:
    raw_results, context = run_checks()
    results = [r.__dict__ for r in raw_results]
    idx = _index_results(results)

    claim1_metrics = [
        "sources_scale::food_nodes",
        "sources_scale::disease_nodes",
        "sources_scale::flavor_nodes",
        "sources_scale::chemical_nodes",
        "sources_scale::food_chemical_edges_r1",
        "sources_scale::chemical_disease_edges_r3_plus_r4",
        "sources_scale::chemical_flavor_edges_r5",
    ]
    claim2_metrics = [
        "representation_clusters::motif_omega3_epa_dha",
        "representation_clusters::motif_omega6_linoleic",
        "representation_clusters::motif_anthocyanins",
        "representation_clusters::motif_limonene",
        "representation_clusters::motif_saturated_palmitic",
        "representation_clusters::motif_fructose_glucose",
        "representation_qvalues::cluster_2_omega-3_epa_q",
        "representation_qvalues::cluster_2_omega-3_dha_q",
        "representation_qvalues::cluster_3_omega-6_linoleic_acid_q",
        "representation_qvalues::cluster_6_anthocyanins_q",
        "representation_qvalues::cluster_11_citrus_limonene_q",
        "representation_qvalues::cluster_14_palmitic_signal_q",
        "representation_qvalues::cluster_15_fructose_q",
        "representation_qvalues::cluster_15_glucose_q",
    ]
    claim3_metrics = [
        "substitutions::disease_substitutions_n",
        "substitutions::bioactivity_substitutions_n",
        "substitutions::disease_overall_mean_pct",
        "substitutions::bioactivity_overall_mean_pct",
        "substitutions::disease_breakfast_mean_pct",
        "substitutions::disease_lunch_mean_pct",
        "substitutions::disease_dinner_mean_pct",
        "substitutions::bio_breakfast_mean_pct",
        "substitutions::bio_lunch_mean_pct",
        "substitutions::bio_dinner_mean_pct",
    ]

    # Artifact support checks
    claim1_artifacts = {
        "fig2_circos": _exists(ART / "fig2" / "fig2_circos.svg"),
        "fig2_food_sunburst": _exists(ART / "fig2" / "fig2_food_sunburst.svg"),
        "fig2_chemical_sunburst": _exists(ART / "fig2" / "fig2_chemical_sunburst.svg"),
        "fig2_disease_sunburst": _exists(ART / "fig2" / "fig2_disease_sunburst.svg"),
        "fig2_flavor_sunburst": _exists(ART / "fig2" / "fig2_flavor_sunburst.svg"),
        "fig2_cytobands_csv": _exists(ART / "fig2" / "foodatlas_cytobands.csv"),
    }
    claim2_artifacts = {
        "fig3_main": _exists(PIPELINE / "output" / "cluster_analysis" / "visualizations" / "disease" / PINNED_DISEASE_RUN / "tsne_128_disease_main.svg"),
        "table1_portfolio_report": _exists(PIPELINE / "output" / "cluster_analysis" / "intermediate" / "disease" / PINNED_DISEASE_RUN / "portfolio_report.txt"),
        "table1_risk_features_report": _exists(PIPELINE / "output" / "cluster_analysis" / "intermediate" / "disease" / PINNED_DISEASE_RUN / "tsne_128_risk_features_report.txt"),
    }
    claim3_artifacts = {
        "fig5_boxplots": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "boxplots.svg"),
        "fig5_spider_lollipop": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "spider_lollipop.svg"),
        "fig5_heatmap_breakfast": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "before_after_heatmap_breakfast.svg"),
        "fig5_heatmap_lunch": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "before_after_heatmap_lunch.svg"),
        "fig5_heatmap_dinner": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "before_after_heatmap_dinner.svg"),
        "fig5_sankey_breakfast": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "aggregated_sankey_breakfast.svg"),
        "fig5_sankey_lunch": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "aggregated_sankey_lunch.svg"),
        "fig5_sankey_dinner": _exists(PIPELINE / "output" / "substitutions" / "visualizations" / "aggregated_sankey_dinner.svg"),
        "table2_csv": _exists(PIPELINE / "output" / "substitutions" / "final" / "disease_example_substitutions_table.csv"),
        "table3_csv": _exists(PIPELINE / "output" / "substitutions" / "final" / "bioactivity_example_substitutions_table.csv"),
    }

    def collect(metric_keys: list[str]) -> list[dict[str, Any]]:
        return [idx[k] for k in metric_keys if k in idx]

    c1_res = collect(claim1_metrics)
    c2_res = collect(claim2_metrics)
    c3_res = collect(claim3_metrics)

    def combine_claim_status(result_list: list[dict[str, Any]], artifacts: dict[str, bool]) -> str:
        numeric_status = _status_level([r["status"] for r in result_list])
        artifacts_ok = all(artifacts.values()) if artifacts else False
        if numeric_status == "supported" and artifacts_ok:
            return "supported"
        if numeric_status == "unsupported" or not any(artifacts.values()):
            return "unsupported"
        return "partially_supported"

    audit = {
        "pinned_runs": {
            "disease": PINNED_DISEASE_RUN,
            "bioactivity": PINNED_BIOACTIVITY_RUN,
        },
        "claims": {
            "FoodAtlas incorporates data from a variety of different sources": {
                "status": combine_claim_status(c1_res, claim1_artifacts),
                "metric_results": c1_res,
                "artifact_checks": claim1_artifacts,
            },
            "FoodAtlas knowledge graph encoded meaningful food representation in chemical and health spaces": {
                "status": combine_claim_status(c2_res, claim2_artifacts),
                "metric_results": c2_res,
                "artifact_checks": claim2_artifacts,
            },
            "Holistic food substitutions achieve significant disease-risk reduction and antioxidant gains": {
                "status": combine_claim_status(c3_res, claim3_artifacts),
                "metric_results": c3_res,
                "artifact_checks": claim3_artifacts,
            },
        },
    }
    return audit


def to_markdown(audit: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Paper Support Audit")
    lines.append("")
    lines.append("Claim-level support based on computed metrics and artifact presence.")
    lines.append("")
    lines.append("## Pinned runs")
    lines.append("")
    for k, v in audit["pinned_runs"].items():
        lines.append(f"- `{k}`: `{v}`")
    lines.append("")

    for claim, payload in audit["claims"].items():
        lines.append(f"## {claim}")
        lines.append("")
        lines.append(f"- Status: **{payload['status']}**")
        lines.append("")
        lines.append("### Metrics")
        lines.append("")
        lines.append("| Metric | Manuscript | Observed | Status |")
        lines.append("|---|---:|---:|---|")
        for r in payload["metric_results"]:
            lines.append(f"| `{r['metric']}` | `{r['manuscript_value']}` | `{r['observed_value']}` | **{r['status']}** |")
        lines.append("")
        lines.append("### Artifact Checks")
        lines.append("")
        for name, ok in payload["artifact_checks"].items():
            mark = "OK" if ok else "MISSING"
            lines.append(f"- `{name}`: {mark}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    audit = build_audit()
    md = to_markdown(audit)

    md_path = ROOT / "PAPER_SUPPORT_AUDIT.md"
    json_path = ROOT / "logs" / "paper_support_audit.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md)
    json_path.write_text(json.dumps(audit, indent=2))

    print(f"Wrote markdown audit: {md_path}")
    print(f"Wrote json audit: {json_path}")


if __name__ == "__main__":
    main()
