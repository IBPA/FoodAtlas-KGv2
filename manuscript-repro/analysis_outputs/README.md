# Optional downstream analysis outputs (manuscript figures)

FoodAtlas-KGv2 builds the knowledge graph under `outputs/kg/`. The **npj** manuscript’s downstream analyses (t-SNE / clustering, substitutions, circos-style figures) are **not** produced by the KG construction scripts in this repository.

To use `manuscript-repro/scripts/collect_artifacts.py` and local artifact packaging, place (or symlink) analysis outputs **inside this repository** using the same directory layout as the publication analysis pipeline’s `output/` tree:

```text
analysis_outputs/
├── cluster_analysis/
│   ├── intermediate/disease/<run_id>/portfolio_report.txt
│   ├── intermediate/disease/<run_id>/tsne_*_risk_features_report.txt
│   ├── visualizations/disease/<run_id>/tsne_*_disease_main.svg
│   ├── visualizations/bioactivity/<run_id>/tsne_*_bioactivity.svg
│   └── final/bioactivity/evaluation_curves.svg
├── substitutions/
│   ├── intermediate/{disease,bioactivity}/{breakfast,lunch,dinner}/substitutions.pkl
│   ├── visualizations/*.svg
│   └── final/*_example_substitutions_table.csv
└── visualization/          # Fig. 2–style assets (optional)
    ├── foodatlas_circos_cytoband.svg
    ├── food_sunburst_plot.svg
    └── …
```

Pinned run IDs are defined in `manuscript-repro/scripts/lib_paths.py` (e.g. `run_20251104_non_neonatal_mwu_v1`).

**Override:** set `FOODATLAS_MANUSCRIPT_ANALYSIS_DIR` to an absolute path **inside your clone** if you keep these files elsewhere in the repo (still no references to directories outside the FoodAtlas-KGv2 root).
