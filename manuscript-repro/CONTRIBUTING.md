# Contributing

`manuscript-repro/` is part of [FoodAtlas-KGv2](https://github.com/IBPA/FoodAtlas-KGv2). Keep changes limited to **documentation** and **scripts under `manuscript-repro/scripts/`**.

**Path rule:** do not add dependencies on directories outside the FoodAtlas-KGv2 clone. Use `scripts/lib_paths.py` (`repo_root()`, `kg_dir()`, `analysis_artifacts_root()`).

## Reporting issues

Include your OS, Python version, commit SHA of FoodAtlas-KGv2, and whether `outputs/kg/triplets.tsv` exists. For figure-level checks, note whether `manuscript-repro/analysis_outputs/` is populated.

## Pull requests

- Run: `bash -n scripts/setup_env.sh scripts/run_pipeline.sh`, `python3 -m py_compile scripts/*.py`, **`make test`**, and **`make revalidate`** whenever **`outputs/kg/`** contains graph TSVs (copied or from an earlier run).
- If you change pinned run IDs in `lib_paths.py`, explain why in the PR description.
