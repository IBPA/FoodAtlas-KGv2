# FoodAtlas — Manuscript reproduction

[![Paper](https://img.shields.io/badge/npj%20Science%20of%20Food-Article-00796B?logo=readthedocs)](https://www.nature.com/articles/s41538-025-00680-9)
[![DOI](https://img.shields.io/badge/DOI-10.1038%2Fs41538--025--00680--9-blue)](https://doi.org/10.1038/s41538-025-00680-9)
[![CI](https://img.shields.io/badge/CI-validate.yml-2088FF?logo=githubactions&logoColor=white)](.github/workflows/validate.yml)

Reproducibility package for the paper **“A unified knowledge graph linking foodomics to chemical-disease networks and flavor profiles”** ([Nature Portfolio](https://www.nature.com/articles/s41538-025-00680-9), open access).

This repository does **not** duplicate the full FoodAtlas build. It provides:

- **Pinned workflows** to regenerate analysis outputs from sibling directories `pipeline/` and `visualization_bundle/`.
- **Documentation** mapping [paper sections](#paper-to-reproduction-map) to code, figures, and tables.
- **Verification scripts** that compare recomputed metrics to manuscript claims (exact match not required; directional consistency is validated).

---

## Paper (read online)

| Resource | Link |
|----------|------|
| Article (HTML) | [npj Science of Food](https://www.nature.com/articles/s41538-025-00680-9) |
| DOI | [10.1038/s41538-025-00680-9](https://doi.org/10.1038/s41538-025-00680-9) |
| Abstract | [Jump to abstract](https://www.nature.com/articles/s41538-025-00680-9#Abs1) |
| Results | [Jump to Results](https://www.nature.com/articles/s41538-025-00680-9#Sec2) |
| Methods | [Jump to Methods](https://www.nature.com/articles/s41538-025-00680-9#Sec9) |
| Data availability | [Data availability](https://www.nature.com/articles/s41538-025-00680-9#data-availability) |
| Code availability | [Code availability](https://www.nature.com/articles/s41538-025-00680-9#code-availability) |
| Supplementary information | Linked from the [article page](https://www.nature.com/articles/s41538-025-00680-9) (“Supplementary information”) |

---

## Repository layout

```
foodatlas-manuscript-repro/
├── README.md                 # This file
├── CITATION.cff              # Citation metadata (GitHub “Cite this repository”)
├── LICENSE                   # MIT — this wrapper only (see below)
├── CONTRIBUTING.md           # Issues, PRs, expectations
├── .github/workflows/validate.yml   # CI: shell + Python syntax checks
├── PAPER_SUPPORT_AUDIT.md    # Claim-level support summary (after `make audit`)
├── MANUSCRIPT_CHECK_REPORT.md       # Numeric comparison table (after `make check`)
├── Makefile
├── scripts/
│   ├── setup_env.sh          # Install Python deps from sibling projects
│   ├── run_pipeline.sh       # End-to-end analysis + viz
│   ├── collect_artifacts.py  # Copy key outputs into artifacts/
│   ├── manuscript_check.py   # Compare metrics to manuscript numbers
│   └── paper_support_audit.py
├── artifacts/                # Generated; gitignored — populated by collect
└── logs/                     # Run logs and JSON reports (gitignored)
```

**Sibling dependencies** (expected next to this repo):

- `../pipeline` — preprocessing, cluster analysis, substitution analysis.
- `../visualization_bundle` — circos and sunburst figures (Fig. 2–style outputs).

Clone or arrange your workspace as:

```text
foodatlas/
├── pipeline/
├── visualization_bundle/
└── foodatlas-manuscript-repro/   # this repository
```

---

## Quick start

```bash
cd foodatlas-manuscript-repro

# 1) Environment (installs from ../pipeline and ../visualization_bundle requirements)
make setup

# 2) Full reproduction run (long; requires FoodAtlas data in ../pipeline)
make run

# 3) Collect figures/tables into artifacts/, run checks, write support audit
make audit
```

Individual steps:

| Step | Command | Purpose |
|------|---------|---------|
| Install deps | `make setup` or `bash scripts/setup_env.sh` | `pip install` from sibling `requirements.txt` files |
| Run analyses | `make run` or `bash scripts/run_pipeline.sh` | Preprocessing → clustering → substitutions → `run_all.py` viz |
| Collect outputs | `python3 scripts/collect_artifacts.py` | Copy key SVG/CSV into `artifacts/` |
| Numeric check | `python3 scripts/manuscript_check.py` | Writes `MANUSCRIPT_CHECK_REPORT.md` |
| Full audit | `make audit` | Collect + check + `PAPER_SUPPORT_AUDIT.md` |

---

## Paper-to-reproduction map

Use this table to jump from the **published paper** (stable section anchors on the [Nature HTML version](https://www.nature.com/articles/s41538-025-00680-9)) to **what to run** and **where outputs land**.

| Paper location | Main claim / content | Reproduction in this workflow |
|----------------|----------------------|-------------------------------|
| [**Abstract**](https://www.nature.com/articles/s41538-025-00680-9#Abs1) | Graph scale, extraction F₁, modules, BPM, substitutions | Scale/substitution checks in `manuscript_check.py`; full KG construction and extraction pipelines are in [FoodAtlas-KGv2](https://github.com/IBPA/FoodAtlas-KGv2) and [Lit2KG](https://github.com/IBPA/Lit2KG) per [Code availability](https://www.nature.com/articles/s41538-025-00680-9#code-availability) |
| [**Fig. 1**](https://www.nature.com/articles/s41538-025-00680-9#Fig1) | Pipeline schematic | Conceptual; not generated by a single script in this package |
| [**Sec. “FoodAtlas incorporates data…”**](https://www.nature.com/articles/s41538-025-00680-9#Sec3), [**Fig. 2**](https://www.nature.com/articles/s41538-025-00680-9#Fig2) | Composition of the KG, circos, sunbursts | `../visualization_bundle/run_all.py` → `artifacts/fig2/` after `collect_artifacts.py` |
| [**Sec. “LLM-based information extraction…”**](https://www.nature.com/articles/s41538-025-00680-9#Sec4) | Extraction F₁, prompts | Upstream [Lit2KG](https://github.com/IBPA/Lit2KG); metrics referenced in `manuscript_check.py` where applicable |
| [**Sec. “encoded meaningful food representation…”**](https://www.nature.com/articles/s41538-025-00680-9#Sec5), [**Fig. 3**](https://www.nature.com/articles/s41538-025-00680-9#Fig3), [**Table 1**](https://www.nature.com/articles/s41538-025-00680-9#Tab1) | t-SNE, clusters, chemistry/disease signatures | `cluster_analysis()` in `../pipeline` → `artifacts/fig3_table1/`; pinned run in `PAPER_SUPPORT_AUDIT.md` |
| [**Sec. “predict food-level antioxidant capacity”**](https://www.nature.com/articles/s41538-025-00680-9#Sec6), [**Fig. 4**](https://www.nature.com/articles/s41538-025-00680-9#Fig4) | BPM, bioactivity t-SNE, evaluation curves | `cluster_analysis()` (bioactivity branch) → `artifacts/fig4/` |
| [**Sec. “Holistic food substitutions…”**](https://www.nature.com/articles/s41538-025-00680-9#Sec7), [**Fig. 5**](https://www.nature.com/articles/s41538-025-00680-9#Fig5), [**Table 2**](https://www.nature.com/articles/s41538-025-00680-9#Tab2), [**Table 3**](https://www.nature.com/articles/s41538-025-00680-9#Tab3) | Substitution visuals and example tables | `substitution_analysis()` → `artifacts/fig5_table2_table3/` |
| [**Methods**](https://www.nature.com/articles/s41538-025-00680-9#Sec9) — [BPM](https://www.nature.com/articles/s41538-025-00680-9#Sec17), [one-hop substitutions](https://www.nature.com/articles/s41538-025-00680-9#Sec18) | WWEIA meals, scoring, swap logic, Random Forest BPM | Implemented under `../pipeline/src/pipeline/` (`cluster_analysis.py`, `substitution_analysis.py`, …) |
| [**Data availability**](https://www.nature.com/articles/s41538-025-00680-9#data-availability) | Public datasets and graph releases | [FoodAtlas-KGv2](https://github.com/IBPA/FoodAtlas-KGv2), [foodatlas.ai](https://foodatlas.ai/) |

Figure/table shortcuts: [Fig. 2](https://www.nature.com/articles/s41538-025-00680-9#Fig2) · [Fig. 3](https://www.nature.com/articles/s41538-025-00680-9#Fig3) · [Fig. 4](https://www.nature.com/articles/s41538-025-00680-9#Fig4) · [Fig. 5](https://www.nature.com/articles/s41538-025-00680-9#Fig5) · [Tab. 1](https://www.nature.com/articles/s41538-025-00680-9#Tab1) · [Tab. 2](https://www.nature.com/articles/s41538-025-00680-9#Tab2) · [Tab. 3](https://www.nature.com/articles/s41538-025-00680-9#Tab3).

Concrete output filenames are gathered by **`scripts/collect_artifacts.py`** into `artifacts/`; see that script and `scripts/run_pipeline.sh` for pipeline order.

---

## Verification outputs

After `make audit` (or `make check` + `paper_support_audit.py`):

| File | Description |
|------|-------------|
| `MANUSCRIPT_CHECK_REPORT.md` | Row-by-row metric vs manuscript |
| `PAPER_SUPPORT_AUDIT.md` | Claim-level **supported / partially supported** + artifact checklist |
| `logs/manuscript_check_report.json` | Machine-readable check results |
| `logs/paper_support_audit.json` | Machine-readable claim audit |

Reproduced numbers are intended to be **consistent with the paper’s conclusions** (same direction and significance); **exact** equality is not guaranteed across software versions and pinned runs. See **`PAPER_SUPPORT_AUDIT.md`** (from `make audit`) and **`MANUSCRIPT_CHECK_REPORT.md`** (from `make check`).

---

## Data and third-party sources

- FoodAtlas graph inputs live under `../pipeline/foodatlas/` and related paths as used by that codebase.
- [CTD](https://ctdbase.org/) and other sources may impose **license restrictions**; this repo does not redistribute them. Follow [Data availability](https://www.nature.com/articles/s41538-025-00680-9#data-availability) and upstream repos: [FoodAtlas-KGv2](https://github.com/IBPA/FoodAtlas-KGv2), [Lit2KG](https://github.com/IBPA/Lit2KG).

---

## Citation

If you use this reproduction package, cite the paper. GitHub will show a **Cite this repository** button from [`CITATION.cff`](CITATION.cff) once this repo is public. Update `repository-code` in `CITATION.cff` if the canonical URL is not `github.com/IBPA/foodatlas-manuscript-repro`.

The manuscript itself is open access under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (see the [article](https://www.nature.com/articles/s41538-025-00680-9)).

---

## License

Scripts and documentation in **this** repository are released under the **MIT License** (`LICENSE`). The FoodAtlas dataset, upstream `pipeline/` code, and the journal article PDF/HTML remain under their respective licenses and copyright.

---

## GitHub checklist

When you publish this folder as its own repository:

1. Place it beside `pipeline/` and `visualization_bundle/` as documented above, or document your layout in a short **Wiki** / README note.
2. Set the default branch to **`main`** (the CI workflow is wired for `main` / `master`).
3. Edit **`repository-code`** in [`CITATION.cff`](CITATION.cff) if the GitHub URL differs from `IBPA/foodatlas-manuscript-repro`.
4. After the first push, you can swap the static CI badge for a **workflow status** badge from GitHub (Actions → validate workflow → “Create status badge”).
5. From the paper’s [Code availability](https://www.nature.com/articles/s41538-025-00680-9#code-availability) section, add a sentence linking to this repo once the URL is final (coordinate with the journal if you amend the HTML statement).

---

## Contributing

See **`CONTRIBUTING.md`**.
