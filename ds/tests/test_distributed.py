import os, sys, tempfile, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ml"))
from service import Node

CONFIG = {"nodes": {"A": {"priority": 1}, "B": {"priority": 2}}}

class DistributedStructuresTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.db = os.path.join(self.tmp.name, "a.db")
        self.node = Node("A", CONFIG, self.db, os.path.join(self.tmp.name, "missing.csv"))
    def tearDown(self): self.tmp.cleanup()
    def test_lamport_idempotency_and_recovery(self):
        event = self.node._event("listing.created", {"listing_id": "L1", "title": "Room"}, propagate=False)
        self.assertEqual(event["lamport"], 1); self.assertFalse(self.node.receive(event)); self.assertEqual(len(self.node.listings), 1)
        recovered = Node("B", CONFIG, os.path.join(self.tmp.name, "b.db"), os.path.join(self.tmp.name, "missing.csv"))
        self.assertTrue(recovered.receive(event)); self.assertEqual(recovered.listings["L1"]["title"], "Room")
    def test_optimistic_profile_versions(self):
        self.node._event("profile.updated", {"user_id": "u", "name": "first"}, version=1, propagate=False)
        self.node._event("profile.updated", {"user_id": "u", "name": "stale"}, version=0, propagate=False)
        self.assertEqual(self.node.profiles["u"]["name"], "first")
    def test_deterministic_reservation_conflict(self):
        self.node._event("listing.created", {"listing_id": "L1"}, propagate=False)
        self.node._event("reservation.requested", {"reservation_id": "r2", "listing_id": "L1", "user_id": "u2"}, version=20, origin="B", propagate=False)
        self.node._event("reservation.requested", {"reservation_id": "r1", "listing_id": "L1", "user_id": "u1"}, version=10, origin="A", propagate=False)
        self.assertEqual(self.node.reservations["L1"]["user_id"], "u1")
    def test_listing_update_and_search_state(self):
        self.node._event("listing.created", {"listing_id": "L1", "location": "Kathmandu", "monthly_rent": 10000}, version=1, propagate=False)
        self.node._event("listing.updated", {"listing_id": "L1", "location": "Kathmandu", "monthly_rent": 9000}, version=2, propagate=False)
        self.assertEqual(self.node.listings["L1"]["monthly_rent"], 9000)
    def test_subscription_filters_notification_delivery(self):
        self.node._event("subscription.created", {"user_id": "u", "event_kind": "recommendation.generated"}, propagate=False)
        self.node._event("notification.sent", {"user_id": "u", "event_kind": "other.event"}, propagate=False)
        self.node._event("notification.sent", {"user_id": "u", "event_kind": "recommendation.generated"}, propagate=False)
        self.assertEqual([x["delivery_status"] for x in self.node.notifications], ["queued", "delivered"])
    def test_versioned_scoring_config(self):
        self.node._event("scoring.config.updated", {"weights": {"location": .5}}, version=2, propagate=False)
        self.assertEqual(self.node.scoring_config["version"], 2); self.assertEqual(self.node.scoring_config["weights"]["location"], .5)

if __name__ == "__main__": unittest.main(verbosity=2)
