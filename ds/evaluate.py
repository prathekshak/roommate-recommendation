#!/usr/bin/env python3
"""Run the proposal's reproducible core and stretch-goal checks."""
import json, os, sys, tempfile
from concurrent.futures import ThreadPoolExecutor
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); sys.path.insert(0, os.path.join(ROOT, "ml")); sys.path.insert(0, os.path.join(ROOT, "ds"))
from recommender import recommend
from preprocess import clean
from service import Node

def main():
    raw = os.path.join(ROOT, "ml", "data", "raw", "roommate_dataset.csv")
    processed = os.path.join(ROOT, "ml", "data", "processed")
    clean(raw, os.path.join(processed, "roommate_clean.csv"), os.path.join(processed, "roommate_clean.json"), os.path.join(processed, "summary.json"))
    rows = json.load(open(os.path.join(processed, "roommate_clean.json"), encoding="utf-8")); profile = rows[0]
    with tempfile.TemporaryDirectory() as tmp:
        cfg = {"nodes": {"A": {"priority": 1}, "B": {"priority": 2}, "C": {"priority": 3}}}
        a = Node("A", cfg, os.path.join(tmp, "a.db"), os.path.join(tmp, "missing.csv")); b = Node("B", cfg, os.path.join(tmp, "b.db"), os.path.join(tmp, "missing.csv"))
        event = a._event("listing.created", {"listing_id": "evaluation-listing", "location": "Kathmandu"}, version=1, propagate=False); replicated = b.receive(event) and b.receive(event) is False
        a._event("subscription.created", {"user_id": "u", "event_kind": "recommendation.generated"}, propagate=False)
        a._event("notification.sent", {"user_id": "u", "event_kind": "recommendation.generated"}, propagate=False)
        notification_delivered = a.notifications[-1]["delivery_status"] == "delivered"
        a._event("listing.created", {"listing_id": "reserved", "location": "Kathmandu"}, propagate=False)
        a._event("reservation.requested", {"reservation_id": "r2", "listing_id": "reserved", "user_id": "u2"}, version=20, origin="B", propagate=False)
        a._event("reservation.requested", {"reservation_id": "r1", "listing_id": "reserved", "user_id": "u1"}, version=10, origin="A", propagate=False)
        reservation_conflict_resolved = a.reservations["reserved"]["user_id"] == "u1"
        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(lambda _: a.batcher.submit(profile, rows, 5), range(10)))
        for _ in range(3): a.mark_peer_failure("C")
        consistency_check = b.listings.get("evaluation-listing") == a.listings.get("evaluation-listing")
        result = {"profiles": len(rows), "batcher": a.batcher.metrics, "event_replicated_and_deduplicated": replicated and "evaluation-listing" in b.listings, "notification_delivered": notification_delivered, "reservation_conflict_resolved": reservation_conflict_resolved, "failure_alerts": len(a.failure_alerts), "leader_after_C_failure": a.leader(), "consistency_check": consistency_check, "core_passed": all([len(rows) == 10000, replicated, notification_delivered, reservation_conflict_resolved, consistency_check])}
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    json.dump(result, open(os.path.join(ROOT, "results", "evaluation_summary.json"), "w"), indent=2)
    print(json.dumps(result, indent=2))
    if not result["core_passed"]: raise SystemExit(1)

if __name__ == "__main__": main()
