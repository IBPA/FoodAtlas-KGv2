# FoodAtlas — Manuscript reproduction (`manuscript-repro/`)

[![Paper](https://img.shields.io/badge/npj%20Science%20of%20Food-Article-00796B?logo=readthedocs)](https://www.nature.com/articles/s41538-025-00680-9)
[![DOI](https://img.shields.io/badge/DOI-10.1038%2Fs41538--025--00680--9-blue)](https://doi.org/10.1038/s41538-025-00680-9)

Reproducibility helpers for **“A unified knowledge graph linking foodomics to chemical-disease networks and flavor profiles”** ([article](https://www.nature.com/articles/s41538-025-00680-9)).

This folder lives **inside** [FoodAtlas-KGv2](https://github.com/IBPA/FoodAtlas-KGv2). **All paths are resolved within the repository clone** — there are no references to sibling directories such as `pipeline/` or `visualization_bundle/` outside this repo.

---

## What runs where

| Component | Location in this repo |
|-----------|------------------------|
| **Knowledge graph files** | Expected under **`outputs/kg/`** (e.g. `entities.tsv`, `triplets.tsv`). You can **place existing exports** there or generate them via `./scripts/0_run_kg_init.sh` and follow-on stages (see root `README.md`). |
| **Graph scale checks** (entities / triplets) | **`make revalidate`** reads whatever is already in **`outputs/kg/`** — no rebuild step; empty or missing files yield a skip message. Generated TSVs are **gitignored**—see [`outputs/README.md`](../outputs/README.md). |
| **Downstream manuscript figures** (Fig. 2–5 style clustering, substitutions, circos) | **Not** produced by the KG construction code here. Supply outputs under `manuscript-repro/analysis_outputs/` (see `analysis_outputs/README.md`) or set `FOODATLAS_MANUSCRIPT_ANALYSIS_DIR` to another directory **inside your clone**. |

---

## Quick start

From the **FoodAtlas-KGv2** repository root:

```bash
cd manuscript-repro

make setup    # pip install -r ../requirements.txt
make run      # KG sanity check + pointers to KG scripts (see scripts/run_pipeline.sh)
make fig2     # generate Fig.2 circos/sunburst assets into analysis_outputs/visualization
make collect  # copy assets into artifacts/ when analysis_outputs/ is populated
make revalidate  # revalidate_paper.py (Fig. 2 scale metrics vs manuscript)
make clean    # remove generated logs/artifacts
```

Run targets assume your current directory is **`manuscript-repro/`** (as with `make` elsewhere in the project).

### Optional unit tests (local)

From `manuscript-repro/`:

```bash
PYTHONPATH=scripts python3 -m unittest discover -s tests -v
```

This runs `unittest` against:

1. **Fixture KG** (`tests/fixtures/kg/`) — asserts `_core_graph_metrics()` and **sources_scale** rows from `run_checks()` match **fixed expected counts**, and that metrics are **identical across repeated calls** (deterministic code path).

2. **Paper revalidation** (optional) — if `../outputs/kg/triplets.tsv` exists and has **at least one data row**, `tests/test_paper_revalidation.py` runs and requires every **sources_scale** check to be **match** or **close** vs the npj manuscript (same tolerances as `manuscript_check.py`, e.g. chemical nodes within 8%). Uses graph files **already in** `outputs/kg/`; if that file is empty or absent, the test is **skipped**.

### Fig.2 asset generation (circos + sunbursts)

`make fig2` runs `scripts/generate_fig2_assets.py`, which wraps the existing visualization generator used in the analysis pipeline and copies the following outputs into `analysis_outputs/visualization/`:

- `foodatlas_circos_cytoband.svg`
- `foodatlas_cytobands.csv`
- `food_sunburst_plot.svg`
- `chemical_sunburst_plot.svg`
- `disease_sunburst_plot.svg`
- `flavor_sunburst_plot.svg`

By default it looks for a sibling pipeline checkout at `../pipeline`; override with `FOODATLAS_PIPELINE_ROOT=/abs/path/to/pipeline`.

**`make revalidate`** runs **`scripts/revalidate_paper.py`**, which reads **`outputs/kg/`** files and prints each scale metric (exits non-zero on **mismatch**/**missing**). If there are no triplets yet, it prints `SKIP` and exits 0 — you only need to **add** graph exports under `outputs/kg/`, not rerun the full build pipeline.

Optional: set **`FOODATLAS_KG_DIR`** to point `manuscript_check` at a different KG directory (tests set this for the fixture; clear it for real `outputs/kg/`).

---

## Repository layout (this folder)

```
manuscript-repro/
├── README.md
├── CITATION.cff
├── CONTRIBUTING.md
├── LICENSE
├── Makefile
├── analysis_outputs/    # optional: downstream analysis tree (gitignored except README)
├── tests/
│   ├── test_reproducibility.py
│   ├── test_paper_revalidation.py
│   └── fixtures/kg/       # tiny KG TSVs for local unittest
├── scripts/
│   ├── lib_paths.py       # repo-internal paths only
│   ├── setup_env.sh
│   ├── run_pipeline.sh    # driver + docs (does not call external repos)
│   ├── generate_fig2_assets.py
│   ├── collect_artifacts.py
│   └── revalidate_paper.py
├── artifacts/             # gitignored — created by collect
└── logs/                  # gitignored — runtime logs
```

---

## Paper-to-reproduction map

| Paper (HTML) | Content | In this repo |
|--------------|---------|----------------|
| [**Abstract**](https://www.nature.com/articles/s41538-025-00680-9#Abs1) | Scale, extraction, analyses | KG: `outputs/kg/`; extraction: [Lit2KG](https://github.com/IBPA/Lit2KG) per [Code availability](https://www.nature.com/articles/s41538-025-00680-9#code-availability) |
| [**Fig. 2**](https://www.nature.com/articles/s41538-025-00680-9#Fig2) | Circos / sunbursts | Place exports under `analysis_outputs/visualization/` → `collect_artifacts.py` |
| [**Fig. 3–5**, **Tables 1–3**](https://www.nature.com/articles/s41538-025-00680-9#Fig3) | Clustering, BPM, substitutions | Populate `analysis_outputs/` per `analysis_outputs/README.md` |

Figure shortcuts: [Fig. 2](https://www.nature.com/articles/s41538-025-00680-9#Fig2) · [Fig. 3](https://www.nature.com/articles/s41538-025-00680-9#Fig3) · [Fig. 4](https://www.nature.com/articles/s41538-025-00680-9#Fig4) · [Fig. 5](https://www.nature.com/articles/s41538-025-00680-9#Fig5)

---

## Data and licenses

- KG construction may use restricted sources (e.g. [CTD](https://ctdbase.org/)); follow the paper [Data availability](https://www.nature.com/articles/s41538-025-00680-9#data-availability).
- The journal article is [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

---

## Citation

See [`CITATION.cff`](CITATION.cff). Prefer citing the **npj** paper by DOI; the `repository-code` field points at this monorepo.

---

## License

Scripts and docs in **`manuscript-repro/`** use the **MIT** [`LICENSE`](LICENSE). The rest of FoodAtlas-KGv2 uses **Apache-2.0** (see root `LICENSE`).

---

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).
