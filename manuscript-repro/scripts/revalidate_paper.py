#!/usr/bin/env python3
"""
Revalidate manuscript Fig. 2 scale metrics against the graph files in outputs/kg/.

Uses whatever `entities.tsv` and `triplets.tsv` are already present (e.g. copied in or
produced earlier). Does **not** run the KG pipeline or rebuild the graph.

Requires at least one data row in `outputs/kg/triplets.tsv`.
Uses the same tolerances as manuscript_check.py (e.g. chemical_nodes within 8%).

Exit codes: 0 = all scale checks match or close; 1 = mismatch/missing; 0 + SKIP if no triplets.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
MR = SCRIPTS.parent
sys.path.insert(0, str(SCRIPTS))


def _kg_populated() -> bool:
    import pandas as pd

    from lib_paths import kg_dir

    p = kg_dir() / "triplets.tsv"
    if not p.exists():
        return False
    try:
        df = pd.read_csv(p, sep="\t")
    except Exception:
        return False
    return len(df) > 0


def main() -> int:
    if not _kg_populated():
        print(
            "SKIP: outputs/kg/triplets.tsv is missing or has no data rows.\n"
            "Place existing graph exports under outputs/kg/ (entities.tsv, triplets.tsv, …), "
            "then run again — revalidation does not rebuild the graph.",
            file=sys.stderr,
        )
        return 0

    from manuscript_check import run_checks

    results, context = run_checks()
    scale = [r for r in results if r.claim == "sources_scale"]
    if not scale:
        print("FAIL: no sources_scale results", file=sys.stderr)
        return 1

    print("Manuscript scale revalidation (Fig. 2 subgraph metrics vs paper):\n")
    print(f"  kg_dir: {context['paths'].get('kg_dir', '')}\n")

    bad: list[str] = []
    for r in scale:
        line = f"  {r.metric}: manuscript={r.manuscript_value!r} observed={r.observed_value!r} -> {r.status}"
        if r.note:
            line += f" ({r.note})"
        print(line)
        if r.status in ("mismatch", "missing"):
            bad.append(r.metric)

    print()
    if bad:
        print(f"FAIL: {len(bad)} metric(s) not match/close: {bad}", file=sys.stderr)
        return 1

    print("OK: all sources_scale metrics match or are within tolerance vs the paper.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
