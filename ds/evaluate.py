#!/usr/bin/env python3
"""Reproducible local evaluation of scoring, batching, replication, and faults."""
import json, os, sys, tempfile, time
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); sys.path.insert(0, os.path.join(ROOT, "ml")); sys.path.insert(0, os.path.join(ROOT, "ds"))
from recommender import recommend
from service import Node

def main():
    rows = json.load(open(os.path.join(ROOT, "ml", "data", "processed", "roommate_clean.json"), encoding="utf-8")); profile = rows[0]
    started = time.perf_counter(); [recommend(profile, rows, 5) for _ in range(10)]; sequential = time.perf_counter() - started
    with tempfile.TemporaryDirectory() as tmp:
        cfg = {"nodes": {"A": {"priority": 1}, "B": {"priority": 2}, "C": {"priority": 3}}}
        a = Node("A", cfg, os.path.join(tmp, "a.db"), os.path.join(tmp, "missing.csv")); b = Node("B", cfg, os.path.join(tmp, "b.db"), os.path.join(tmp, "missing.csv"))
        for _ in range(10): a.batcher.submit(profile, rows, 5)
        event = a._event("listing.created", {"listing_id": "evaluation-listing", "location": "Kathmandu"}, version=1, propagate=False); replicated = b.receive(event)
        for _ in range(3): a.mark_peer_failure("C")
        result = {"profiles": len(rows), "sequential_10_requests_seconds": round(sequential, 4), "batcher": a.batcher.metrics, "event_replicated": replicated and "evaluation-listing" in b.listings, "failure_alerts": len(a.failure_alerts), "leader_after_C_failure": a.leader(), "consistency_check": a.listings == b.listings}
    print(json.dumps(result, indent=2))

if __name__ == "__main__": main()
