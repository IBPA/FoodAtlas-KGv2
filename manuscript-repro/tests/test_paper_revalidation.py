"""
Optional revalidation against the published manuscript scale metrics.

Runs only when ``outputs/kg/triplets.tsv`` exists and contains at least one row
(graph files already present — no rebuild required). Clears ``FOODATLAS_KG_DIR``
so the real repo ``outputs/kg/`` is used.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

MR = Path(__file__).resolve().parents[1]
SCRIPTS = MR / "scripts"
REPO_ROOT = MR.parent


def _outputs_kg_populated() -> bool:
    p = REPO_ROOT / "outputs" / "kg" / "triplets.tsv"
    if not p.exists():
        return False
    try:
        import pandas as pd

        df = pd.read_csv(p, sep="\t")
    except Exception:
        return False
    return len(df) > 0


@unittest.skipUnless(
    _outputs_kg_populated(),
    "outputs/kg/triplets.tsv missing or empty — add graph files under outputs/kg/ to enable paper revalidation",
)
class TestPaperScaleRevalidation(unittest.TestCase):
    def setUp(self) -> None:
        self._prev = os.environ.get("FOODATLAS_KG_DIR")
        os.environ.pop("FOODATLAS_KG_DIR", None)
        if str(SCRIPTS) not in sys.path:
            sys.path.insert(0, str(SCRIPTS))

    def tearDown(self) -> None:
        if self._prev is None:
            os.environ.pop("FOODATLAS_KG_DIR", None)
        else:
            os.environ["FOODATLAS_KG_DIR"] = self._prev

    def test_sources_scale_match_or_close_to_manuscript(self) -> None:
        from manuscript_check import run_checks

        results, _ = run_checks()
        scale = [r for r in results if r.claim == "sources_scale"]
        self.assertTrue(scale, "expected sources_scale rows")
        bad = [r for r in scale if r.status in ("mismatch", "missing")]
        self.assertEqual(
            bad,
            [],
            "Fig. 2 scale metrics should match or be within tolerance vs the paper: "
            + ", ".join(f"{r.metric}={r.status}" for r in bad),
        )


if __name__ == "__main__":
    unittest.main()
