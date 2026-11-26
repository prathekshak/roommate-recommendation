import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ml"))
from generate_dataset import generate
from preprocess import clean


class DataPipelineTest(unittest.TestCase):
    def test_deterministic_dataset_bootstrap_and_cleaning(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = os.path.join(tmp, "raw.csv")
            out = os.path.join(tmp, "processed")
            generate(raw, rows=25, seed=249)
            clean(raw, os.path.join(out, "clean.csv"), os.path.join(out, "clean.json"), os.path.join(out, "summary.json"))
            with open(os.path.join(out, "clean.json"), encoding="utf-8") as stream:
                rows = json.load(stream)
            with open(os.path.join(out, "summary.json"), encoding="utf-8") as stream:
                summary = json.load(stream)
            self.assertEqual(len(rows), 25)
            self.assertEqual(summary["rows"], 25)
            self.assertEqual(rows[0]["user_id"], "100000")


if __name__ == "__main__":
    unittest.main(verbosity=2)
