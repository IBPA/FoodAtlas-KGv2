# `scripts/` overview

Helper scripts used by `manuscript-repro` Make targets.

## Files

- `setup_env.sh`  
  Installs Python dependencies for reproduction (`../requirements.txt`).

- `run_pipeline.sh`  
  Repository-local driver notes for KG sanity checks and downstream artifact expectations.

- `lib_paths.py`  
  Canonical path helpers for repo-root, KG location, and analysis output location.

- `generate_fig2_assets.py`  
  Produces (or copies fallback) Fig.2 visualization assets into `analysis_outputs/visualization/`.

- `collect_artifacts.py`  
  Copies expected figure/table assets into `artifacts/` for local review.

- `revalidate_paper.py`  
  Revalidates manuscript scale metrics from `outputs/kg/` against reference values.

## Notes

- These scripts are intended for local reproduction workflows.
- Generated outputs are gitignored under `artifacts/`, `logs/`, and `analysis_outputs/` (except README files).
