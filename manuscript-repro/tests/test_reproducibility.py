"""
Reproducibility tests for manuscript-repro scale checks.

Uses a tiny committed KG fixture under tests/fixtures/kg/ and FOODATLAS_KG_DIR
so results do not depend on outputs/kg/ on the developer machine.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

MR = Path(__file__).resolve().parents[1]
SCRIPTS = MR / "scripts"
_FIXTURE_KG = MR / "tests" / "fixtures" / "kg"


class TestManuscriptScaleReproducibility(unittest.TestCase):
    """Deterministic checks against fixed fixture TSVs."""

    def setUp(self) -> None:
        self._prev_kg = os.environ.get("FOODATLAS_KG_DIR")
        os.environ["FOODATLAS_KG_DIR"] = str(_FIXTURE_KG)
        if str(SCRIPTS) not in sys.path:
            sys.path.insert(0, str(SCRIPTS))

    def tearDown(self) -> None:
        if self._prev_kg is None:
            os.environ.pop("FOODATLAS_KG_DIR", None)
        else:
            os.environ["FOODATLAS_KG_DIR"] = self._prev_kg

    def test_core_graph_metrics_match_fixture(self) -> None:
        from manuscript_check import _core_graph_metrics

        core = _core_graph_metrics()
        self.assertFalse(core.get("kg_missing"))
        ec = core["entity_counts"]
        rc = core["relation_counts"]
        # Fixture: one food (f1), one chemical, one disease, one flavor in the r1/r3/r5 core subgraph.
        self.assertEqual(ec.get("food"), 1)
        self.assertEqual(ec.get("chemical"), 1)
        self.assertEqual(ec.get("disease"), 1)
        self.assertEqual(ec.get("flavor"), 1)
        self.assertEqual(rc.get("r1"), 1)
        self.assertEqual(rc.get("r3"), 1)
        self.assertEqual(rc.get("r5"), 1)
        self.assertEqual(core["chem_disease_total"], 1)

    def test_core_graph_metrics_stable_across_calls(self) -> None:
        """Same inputs must yield identical computed metrics (reproducibility)."""
        from manuscript_check import _core_graph_metrics

        a = _core_graph_metrics()
        b = _core_graph_metrics()
        self.assertEqual(a, b)

    def test_run_checks_sources_scale_observed_values(self) -> None:
        from manuscript_check import run_checks

        results, _ctx = run_checks()
        scale = [r for r in results if r.claim == "sources_scale"]
        self.assertEqual(len(scale), 7)
        by_metric = {r.metric: r.observed_value for r in scale}
        self.assertEqual(by_metric["food_nodes"], 1)
        self.assertEqual(by_metric["disease_nodes"], 1)
        self.assertEqual(by_metric["flavor_nodes"], 1)
        self.assertEqual(by_metric["chemical_nodes"], 1)
        self.assertEqual(by_metric["food_chemical_edges_r1"], 1)
        self.assertEqual(by_metric["chemical_disease_edges_r3_plus_r4"], 1)
        self.assertEqual(by_metric["chemical_flavor_edges_r5"], 1)


if __name__ == "__main__":
    unittest.main()
