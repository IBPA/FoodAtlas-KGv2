# Contributing

This repository is a thin reproducibility layer around the FoodAtlas analysis code in sibling directories (`pipeline/`, `visualization_bundle/`). Contributions here should stay focused on **documentation**, **reproducibility scripts**, and **verification checks**.

## Reporting issues

When something fails to reproduce:

1. Note your **OS**, **Python version**, and **commit hashes** of this repo and of `pipeline/` / `visualization_bundle/`.
2. Attach or summarize the **last 50 lines** of the failing command’s output.
3. State whether you ran **`make setup`** successfully and whether **input data** paths match what `../pipeline` expects.

## Pull requests

- Keep changes **minimal** and aligned with the published paper ([npj Science of Food](https://www.nature.com/articles/s41538-025-00680-9)).
- If you change thresholds or pinned run IDs in `manuscript_check.py` or `paper_support_audit.py`, update **`PAPER_SUPPORT_AUDIT.md`** (or local notes you keep outside this repo) so readers understand why.
- Run locally before opening a PR:
  - `bash -n scripts/setup_env.sh scripts/run_pipeline.sh`
  - `python3 -m py_compile scripts/*.py`
  - If you have data: `make audit`

## Code of conduct

Be respectful and constructive. For substantive scientific questions about FoodAtlas, prefer citing the paper and contacting the authors via the correspondence address on the article.
