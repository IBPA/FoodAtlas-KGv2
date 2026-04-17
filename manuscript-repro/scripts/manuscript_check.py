#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import pickle
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = ROOT.parent
PIPELINE = PROJECT_ROOT / "pipeline"

# Pinned run snapshots used for deterministic manuscript checks.
PINNED_DISEASE_RUN = "run_20251104_non_neonatal_mwu_v1"
PINNED_BIOACTIVITY_RUN = "run_20251104_non_neonatal_mwu_v1"


@dataclass
class CheckResult:
    claim: str
    metric: str
    manuscript_value: Any
    observed_value: Any
    status: str
    note: str = ""


def _status_for_number(
    observed: float | int | None,
    expected: float | int,
    *,
    rel_tol: float = 0.0,
    abs_tol: float = 0.0,
) -> str:
    if observed is None:
        return "missing"
    if math.isclose(float(observed), float(expected), rel_tol=0.0, abs_tol=0.0):
        return "match"
    if math.isclose(float(observed), float(expected), rel_tol=rel_tol, abs_tol=abs_tol):
        return "close"
    return "mismatch"


def _core_graph_metrics() -> dict[str, Any]:
    entities = pd.read_csv(
        PIPELINE / "foodatlas" / "v3.1" / "entities.tsv",
        sep="\t",
        usecols=["foodatlas_id", "entity_type"],
    )
    triplets = pd.read_csv(
        PIPELINE / "foodatlas" / "v3.1" / "triplets.tsv",
        sep="\t",
        usecols=["head_id", "tail_id", "relationship_id"],
    )

    core = triplets[triplets["relationship_id"].isin(["r1", "r3", "r4", "r5"])]
    rel_counts = core["relationship_id"].value_counts().to_dict()

    node_ids = set(core["head_id"]).union(set(core["tail_id"]))
    core_entities = entities[entities["foodatlas_id"].isin(node_ids)]
    entity_counts = core_entities["entity_type"].value_counts().to_dict()

    return {
        "entity_counts": entity_counts,
        "relation_counts": rel_counts,
        "chem_disease_total": int(rel_counts.get("r3", 0) + rel_counts.get("r4", 0)),
    }


def _load_portfolio_text() -> str:
    path = (
        PIPELINE
        / "output"
        / "cluster_analysis"
        / "intermediate"
        / "disease"
        / PINNED_DISEASE_RUN
        / "portfolio_report.txt"
    )
    return path.read_text()


def _cluster_motif_checks(portfolio_text: str) -> dict[str, bool]:
    motifs = {
        "omega3_epa_dha": ("icosapentaenoic acid" in portfolio_text.lower())
        and ("dha" in portfolio_text.lower()),
        "omega6_linoleic": "linoleic acid" in portfolio_text.lower(),
        "anthocyanins": "anthocyanins" in portfolio_text.lower(),
        "limonene": "limonene" in portfolio_text.lower(),
        "saturated_palmitic": ("sfa 16:0" in portfolio_text.lower())
        or ("hexadecanoic acid" in portfolio_text.lower()),
        "fructose_glucose": ("fructose" in portfolio_text.lower())
        and ("glucose" in portfolio_text.lower()),
    }
    return motifs


def _parse_portfolio_qvals(portfolio_text: str) -> dict[int, dict[str, float]]:
    """
    Parse q-values from portfolio_report.txt.
    Returns: {cluster_id: {chemical_name_lower: q_value}}
    """
    out: dict[int, dict[str, float]] = {}
    current_cluster: int | None = None

    for raw in portfolio_text.splitlines():
        line = raw.strip()
        m_cluster = re.match(r"^Cluster\s+(\d+)", line)
        if m_cluster:
            current_cluster = int(m_cluster.group(1))
            out.setdefault(current_cluster, {})
            continue

        if current_cluster is None:
            continue

        # Example line:
        # – limonene (ID e59728)  Δ=+59.3075 q=6.1e-22  mean_in=54.1960
        m_q = re.match(
            r"^[–-]\s+(.+?)\s+\(ID\s+[^\)]+\)\s+Δ=.*?\s+q=([0-9.eE+-]+)\s+mean_in=.*$",
            line,
        )
        if not m_q:
            continue
        chem_name = m_q.group(1).strip().lower()
        q_val = float(m_q.group(2))
        out[current_cluster][chem_name] = q_val

    return out


def _find_q_for_chem(
    q_map: dict[int, dict[str, float]],
    cluster: int,
    chem_aliases: list[str],
) -> float | None:
    chem_dict = q_map.get(cluster, {})
    for alias in chem_aliases:
        alias_l = alias.lower()
        # direct match first
        if alias_l in chem_dict:
            return chem_dict[alias_l]
        # then substring fallback
        for k, v in chem_dict.items():
            if alias_l in k:
                return v
    return None


def _q_status(observed: float | None, expected: float) -> str:
    if observed is None:
        return "missing"
    if math.isclose(observed, expected, rel_tol=0.0, abs_tol=0.0):
        return "match"
    # Compare in log-space for scientific-notation q-values.
    log_diff = abs(math.log10(observed) - math.log10(expected))
    if log_diff <= 0.5:
        return "close"
    return "mismatch"


def _substitution_metrics() -> dict[str, Any]:
    out: dict[str, Any] = {}
    base = PIPELINE / "output" / "substitutions" / "intermediate"

    for mode in ["disease", "bioactivity"]:
        all_values: list[float] = []
        meal_metrics: dict[str, dict[str, float | int]] = {}
        total_records = 0
        for meal in ["breakfast", "lunch", "dinner"]:
            recs = pickle.load(open(base / mode / meal / "substitutions.pkl", "rb"))
            total_records += len(recs)
            vals = [
                float(r.get("overall_pct_change"))
                for r in recs
                if isinstance(r.get("overall_pct_change"), (int, float))
                and math.isfinite(r.get("overall_pct_change"))
            ]
            all_values.extend(vals)
            meal_metrics[meal] = {
                "n": len(recs),
                "mean": (sum(vals) / len(vals)) if vals else float("nan"),
            }

        out[mode] = {
            "n_total": total_records,
            "overall_mean": (sum(all_values) / len(all_values)) if all_values else float("nan"),
            "by_meal": meal_metrics,
        }
    return out


def run_checks() -> tuple[list[CheckResult], dict[str, Any]]:
    results: list[CheckResult] = []

    core = _core_graph_metrics()
    entities = core["entity_counts"]
    relations = core["relation_counts"]

    # Claim 1: data variety / scale
    results.extend(
        [
            CheckResult(
                claim="sources_scale",
                metric="food_nodes",
                manuscript_value=1430,
                observed_value=int(entities.get("food", 0)),
                status=_status_for_number(int(entities.get("food", 0)), 1430),
            ),
            CheckResult(
                claim="sources_scale",
                metric="disease_nodes",
                manuscript_value=3177,
                observed_value=int(entities.get("disease", 0)),
                status=_status_for_number(int(entities.get("disease", 0)), 3177),
            ),
            CheckResult(
                claim="sources_scale",
                metric="flavor_nodes",
                manuscript_value=1117,
                observed_value=int(entities.get("flavor", 0)),
                status=_status_for_number(int(entities.get("flavor", 0)), 1117),
            ),
            CheckResult(
                claim="sources_scale",
                metric="chemical_nodes",
                manuscript_value=10266,
                observed_value=int(entities.get("chemical", 0)),
                status=_status_for_number(int(entities.get("chemical", 0)), 10266, rel_tol=0.08),
                note="Fig.2 caption reference value; marked close if within 8%",
            ),
            CheckResult(
                claim="sources_scale",
                metric="food_chemical_edges_r1",
                manuscript_value=48474,
                observed_value=int(relations.get("r1", 0)),
                status=_status_for_number(int(relations.get("r1", 0)), 48474),
            ),
            CheckResult(
                claim="sources_scale",
                metric="chemical_disease_edges_r3_plus_r4",
                manuscript_value=138792,
                observed_value=int(core["chem_disease_total"]),
                status=_status_for_number(int(core["chem_disease_total"]), 138792),
            ),
            CheckResult(
                claim="sources_scale",
                metric="chemical_flavor_edges_r5",
                manuscript_value=6169,
                observed_value=int(relations.get("r5", 0)),
                status=_status_for_number(int(relations.get("r5", 0)), 6169),
            ),
        ]
    )

    # Claim 2: meaningful representation in chemical + health spaces
    portfolio_text = _load_portfolio_text()
    motifs = _cluster_motif_checks(portfolio_text)
    for motif, ok in motifs.items():
        results.append(
            CheckResult(
                claim="representation_clusters",
                metric=f"motif_{motif}",
                manuscript_value=True,
                observed_value=ok,
                status="match" if ok else "mismatch",
                note="Detected in pinned disease portfolio report",
            )
        )

    # Key manuscript q_enrich checks for cluster signatures (Results section).
    q_map = _parse_portfolio_qvals(portfolio_text)
    q_targets = [
        (2, ["all-cis-5,8,11,14,17-icosapentaenoic acid", "epa"], 1.3e-20, "Omega-3 EPA"),
        (2, ["dha"], 4.9e-14, "Omega-3 DHA"),
        (3, ["linoleic acid"], 4.0e-07, "Omega-6 linoleic acid"),
        (6, ["anthocyanins"], 2.3e-05, "Anthocyanins"),
        (11, ["limonene"], 9.2e-24, "Citrus limonene"),
        (14, ["sfa 16:0", "hexadecanoic acid", "palmitic acid"], 4.0e-35, "Palmitic signal"),
        (15, ["fructose"], 2.6e-22, "Fructose"),
        (15, ["glucose"], 2.1e-19, "Glucose"),
    ]
    for cluster_id, aliases, expected_q, label in q_targets:
        observed_q = _find_q_for_chem(q_map, cluster_id, aliases)
        status = _q_status(observed_q, expected_q)
        note = "log10-distance <= 0.5 treated as close" if status == "close" else ""
        if observed_q is None:
            note = f"Cluster {cluster_id} chemical not found in pinned portfolio report"
        results.append(
            CheckResult(
                claim="representation_qvalues",
                metric=f"cluster_{cluster_id}_{label.replace(' ', '_').lower()}_q",
                manuscript_value=expected_q,
                observed_value=observed_q,
                status=status,
                note=note,
            )
        )

    # Claim 3: substitutions
    subs = _substitution_metrics()
    disease = subs["disease"]
    bio = subs["bioactivity"]
    results.extend(
        [
            CheckResult(
                claim="substitutions",
                metric="disease_substitutions_n",
                manuscript_value=14580,
                observed_value=int(disease["n_total"]),
                status=_status_for_number(int(disease["n_total"]), 14580, rel_tol=0.07),
                note="Close threshold 7%",
            ),
            CheckResult(
                claim="substitutions",
                metric="bioactivity_substitutions_n",
                manuscript_value=7798,
                observed_value=int(bio["n_total"]),
                status=_status_for_number(int(bio["n_total"]), 7798, rel_tol=0.07),
                note="Close threshold 7%",
            ),
            CheckResult(
                claim="substitutions",
                metric="disease_overall_mean_pct",
                manuscript_value=11.9,
                observed_value=round(float(disease["overall_mean"]), 3),
                status=_status_for_number(float(disease["overall_mean"]), 11.9, abs_tol=1.5),
                note="Close threshold +/-1.5 percentage points",
            ),
            CheckResult(
                claim="substitutions",
                metric="bioactivity_overall_mean_pct",
                manuscript_value=210.6,
                observed_value=round(float(bio["overall_mean"]), 3),
                status=_status_for_number(float(bio["overall_mean"]), 210.6, abs_tol=15.0),
                note="Close threshold +/-15 percentage points",
            ),
            CheckResult(
                claim="substitutions",
                metric="disease_breakfast_mean_pct",
                manuscript_value=9.9,
                observed_value=round(float(disease["by_meal"]["breakfast"]["mean"]), 3),
                status=_status_for_number(float(disease["by_meal"]["breakfast"]["mean"]), 9.9, abs_tol=1.0),
            ),
            CheckResult(
                claim="substitutions",
                metric="disease_lunch_mean_pct",
                manuscript_value=10.0,
                observed_value=round(float(disease["by_meal"]["lunch"]["mean"]), 3),
                status=_status_for_number(float(disease["by_meal"]["lunch"]["mean"]), 10.0, abs_tol=1.0),
            ),
            CheckResult(
                claim="substitutions",
                metric="disease_dinner_mean_pct",
                manuscript_value=11.1,
                observed_value=round(float(disease["by_meal"]["dinner"]["mean"]), 3),
                status=_status_for_number(float(disease["by_meal"]["dinner"]["mean"]), 11.1, abs_tol=1.0),
            ),
            CheckResult(
                claim="substitutions",
                metric="bio_breakfast_mean_pct",
                manuscript_value=176.4,
                observed_value=round(float(bio["by_meal"]["breakfast"]["mean"]), 3),
                status=_status_for_number(float(bio["by_meal"]["breakfast"]["mean"]), 176.4, abs_tol=15.0),
            ),
            CheckResult(
                claim="substitutions",
                metric="bio_lunch_mean_pct",
                manuscript_value=257.9,
                observed_value=round(float(bio["by_meal"]["lunch"]["mean"]), 3),
                status=_status_for_number(float(bio["by_meal"]["lunch"]["mean"]), 257.9, abs_tol=15.0),
            ),
            CheckResult(
                claim="substitutions",
                metric="bio_dinner_mean_pct",
                manuscript_value=185.8,
                observed_value=round(float(bio["by_meal"]["dinner"]["mean"]), 3),
                status=_status_for_number(float(bio["by_meal"]["dinner"]["mean"]), 185.8, abs_tol=15.0),
            ),
        ]
    )

    context = {
        "pinned_runs": {
            "disease_cluster_run": PINNED_DISEASE_RUN,
            "bioactivity_cluster_run": PINNED_BIOACTIVITY_RUN,
            "disease_cluster_intermediate_dir": str(
                PIPELINE
                / "output"
                / "cluster_analysis"
                / "intermediate"
                / "disease"
                / PINNED_DISEASE_RUN
            ),
            "disease_cluster_visualization_dir": str(
                PIPELINE
                / "output"
                / "cluster_analysis"
                / "visualizations"
                / "disease"
                / PINNED_DISEASE_RUN
            ),
            "bioactivity_cluster_visualization_dir": str(
                PIPELINE
                / "output"
                / "cluster_analysis"
                / "visualizations"
                / "bioactivity"
                / PINNED_BIOACTIVITY_RUN
            ),
        }
    }
    return results, context


def _to_markdown(results: list[CheckResult], context: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Manuscript Reproduction Check")
    lines.append("")
    lines.append("## Pinned run directories")
    lines.append("")
    for k, v in context["pinned_runs"].items():
        lines.append(f"- `{k}`: `{v}`")
    lines.append("")
    lines.append("## Comparison table")
    lines.append("")
    lines.append("| Claim | Metric | Manuscript | Observed | Status | Note |")
    lines.append("|---|---|---:|---:|---|---|")
    for r in results:
        lines.append(
            f"| `{r.claim}` | `{r.metric}` | `{r.manuscript_value}` | `{r.observed_value}` | **{r.status}** | {r.note} |"
        )
    lines.append("")
    status_counts = pd.Series([r.status for r in results]).value_counts().to_dict()
    lines.append("## Status summary")
    lines.append("")
    for k in sorted(status_counts):
        lines.append(f"- `{k}`: {status_counts[k]}")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check manuscript number/claim reproducibility.")
    parser.add_argument(
        "--output",
        default=str(ROOT / "MANUSCRIPT_CHECK_REPORT.md"),
        help="Output markdown report path",
    )
    parser.add_argument(
        "--json-output",
        default=str(ROOT / "logs" / "manuscript_check_report.json"),
        help="Output JSON report path",
    )
    args = parser.parse_args()

    results, context = run_checks()

    md = _to_markdown(results, context)
    output_path = Path(args.output)
    output_path.write_text(md)

    json_path = Path(args.json_output)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(
            {
                "pinned_runs": context["pinned_runs"],
                "results": [r.__dict__ for r in results],
            },
            indent=2,
        )
    )

    print(f"Wrote markdown report: {output_path}")
    print(f"Wrote JSON report: {json_path}")


if __name__ == "__main__":
    main()
