import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import random
import unittest
import data_source as d

BASE = [{"id": str(i), "refresh_date": "2026-10-20", "region": "Dallas", "region_id": "1", "term": f"term{i}", "week": "2026-10-19", "rank": i % 25 + 1, "score": 1.0} for i in range(100)]

class DataQualityTests(unittest.TestCase):
    def test_all_injected_errors_detected(self):
        for kind in d.ALLOWED_ACTION:
            with self.subTest(kind=kind):
                bad = d.inject(kind, BASE, random.Random(42))
                self.assertTrue(d.validate(bad, len(BASE)))
                self.assertEqual(len(BASE), 100)
    def test_routine_fix_revalidates(self):
        for kind in d.ALLOWED_ACTION:
            if kind == "row_count_anomaly": continue
            with self.subTest(kind=kind):
                bad = d.inject(kind, BASE, random.Random(42))
                fixed, quarantined = d.apply_fix(d.ALLOWED_ACTION[kind], bad, BASE)
                self.assertEqual(d.validate(fixed, len(BASE)), [])
                self.assertGreaterEqual(quarantined, 0)
    def test_original_not_mutated(self):
        d.inject("schema_mismatch", BASE, random.Random(42))
        self.assertIn("rank", BASE[0])
        self.assertNotIn("rank_position", BASE[0])

if __name__ == "__main__": unittest.main()
