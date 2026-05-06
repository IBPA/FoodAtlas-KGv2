# `tests/` overview

Test suite for manuscript reproduction utilities.

## Test modules

- `test_reproducibility.py`  
  Deterministic checks against fixture KG files in `tests/fixtures/kg/`.

- `test_paper_revalidation.py`  
  Optional revalidation against real `outputs/kg/` graph files when `triplets.tsv` has data rows.

## Running

From `manuscript-repro/`:

```bash
PYTHONPATH=scripts python3 -m unittest discover -s tests -v
```
