#!/usr/bin/env python3
"""Small reproducible local benchmark for the scoring layer."""
import json, os, sys, time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); sys.path.insert(0, os.path.join(ROOT, "ml"))
from recommender import recommend
with open(os.path.join(ROOT, "ml/data/processed/roommate_clean.json"), encoding="utf-8") as fh: rows = json.load(fh)
start = time.perf_counter(); requests = 20
for i in range(requests): recommend(rows[i], rows, 10)
elapsed = time.perf_counter() - start
print(json.dumps({"requests": requests, "elapsed_seconds": round(elapsed, 4), "requests_per_second": round(requests / elapsed, 2), "candidate_profiles": len(rows)}, indent=2))
