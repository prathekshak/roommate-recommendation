#!/usr/bin/env python3
"""A small three-node event-driven distributed housing service."""
import argparse, json, os, queue, sqlite3, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml"))
from recommender import recommend
from raft import RaftNode

HEARTBEAT_SECONDS = 2.0
FAILURE_THRESHOLD = 3

class RecommendationBatcher:
    def __init__(self, node, workers=2, max_batch=8, window_ms=25):
        self.node = node; self.queue = queue.Queue(); self.max_batch = max_batch; self.window = window_ms / 1000.0
        self.metrics = {"batches": 0, "requests": 0, "max_batch": max_batch}
        for _ in range(workers): threading.Thread(target=self._worker, daemon=True).start()
    def submit(self, profile, candidates, limit):
        done = threading.Event(); item = {"profile": profile, "candidates": candidates, "limit": limit, "done": done, "result": None}; self.queue.put(item); done.wait(); return item["result"]
    def _worker(self):
        while True:
            first = self.queue.get(); batch = [first]; deadline = time.time() + self.window
            while len(batch) < self.max_batch and time.time() < deadline:
                try: batch.append(self.queue.get(timeout=max(0, deadline - time.time())))
                except queue.Empty: break
            self.metrics["batches"] += 1; self.metrics["requests"] += len(batch)
            for item in batch:
                item["result"] = recommend(item["profile"], item["candidates"], item["limit"], self.node.scoring_config); item["done"].set()

class Node:
    def __init__(self, node_id, config, db_path, data_path):
        self.node_id = node_id; self.config = config; self.spec = config["nodes"][node_id]
        self.priority = self.spec.get("priority", 0); self.lock = threading.RLock(); self.peers = {}
        self.db = sqlite3.connect(db_path, check_same_thread=False); self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute("CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, kind TEXT, payload TEXT, origin TEXT, ts REAL)")
            self._ensure_column("lamport", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("version", "INTEGER NOT NULL DEFAULT 0")
        self.clock = self._max_clock(); self.profiles = {}; self.listings = {}; self.reservations = {}
        self.subscriptions = {}; self.notifications = []; self.failure_alerts = []; self.leader_term = 0; self.current_leader = None
        self.scoring_config = {"version": 1, "weights": {"location": .16, "budget": .16, "sleep": .12, "study": .10, "food": .08, "smoking": .08, "drinking": .06, "pets": .06, "cleanliness": .10, "noise": .08, "interests": .06}}
        self.membership = {k: {"alive": True, "misses": 0, "last_seen": time.time()} for k in config["nodes"]}
        self._load_events()
        if not self.profiles and os.path.exists(data_path):
            import csv
            with open(data_path, newline="", encoding="utf-8") as fh: self.profiles = {r["user_id"]: r for r in csv.DictReader(fh)}
        self.current_leader = self.leader(); self.batcher = RecommendationBatcher(self); self.raft = None

    def attach_raft(self):
        self.raft = RaftNode(self.node_id, self.peers, self.db, lambda command: self._event(command["kind"], command["payload"], command.get("version", 0), command.get("id"), command.get("origin"), command.get("lamport"), False, False)); self.raft.start()

    def _ensure_column(self, name, definition):
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(events)")}
        if name not in columns: self.db.execute("ALTER TABLE events ADD COLUMN %s %s" % (name, definition))
    def _max_clock(self): return int(self.db.execute("SELECT COALESCE(MAX(lamport), 0) AS c FROM events").fetchone()["c"])
    def _load_events(self):
        for row in self.db.execute("SELECT kind,payload,origin,lamport,version FROM events ORDER BY lamport,origin,id"):
            self._apply(row["kind"], json.loads(row["payload"]), row["origin"], row["version"])

    def _apply(self, kind, payload, origin, version=0):
        if kind == "profile.updated":
            uid = str(payload["user_id"]); current = self.profiles.get(uid, {})
            effective_version = int(version) if version is not None else int(payload.get("profile_version", 1))
            if effective_version >= int(current.get("profile_version", 0)):
                payload["profile_version"] = effective_version; self.profiles[uid] = payload
        elif kind in {"listing.created", "listing.updated"}:
            lid = str(payload["listing_id"]); current = self.listings.get(lid, {})
            if int(version or payload.get("listing_version", 1)) >= int(current.get("listing_version", 0)):
                payload["listing_version"] = int(version or payload.get("listing_version", 1)); self.listings[lid] = payload
        elif kind == "subscription.created": self.subscriptions.setdefault(str(payload["user_id"]), []).append(payload)
        elif kind == "notification.sent":
            subscriptions = self.subscriptions.get(str(payload.get("user_id")), [])
            delivered = any(s.get("event_kind") == payload.get("event_kind") for s in subscriptions)
            item = dict(payload); item.setdefault("delivery_status", "delivered" if delivered else "queued"); self.notifications.append(item)
        elif kind == "reservation.requested":
            lid = str(payload["listing_id"]); current = self.reservations.get(lid)
            candidate = (int(version or 0), origin, payload["reservation_id"])
            if current is None or candidate < current["order"]:
                self.reservations[lid] = {"listing_id": lid, "user_id": str(payload["user_id"]), "reservation_id": payload["reservation_id"], "status": "accepted", "order": candidate}
        elif kind == "reservation.cancelled": self.reservations.pop(str(payload["listing_id"]), None)
        elif kind == "failure.detected": self.failure_alerts.append(payload)
        elif kind == "failure.recovered": self.failure_alerts.append(payload)
        elif kind == "scoring.config.updated":
            if int(version) >= int(self.scoring_config.get("version", 1)):
                self.scoring_config = {"version": int(version), "weights": payload["weights"]}
        elif kind == "leader.changed":
            if int(payload.get("term", 0)) >= self.leader_term:
                self.leader_term = int(payload.get("term", 0)); self.current_leader = payload.get("leader")

    def _event(self, kind, payload, version=0, event_id=None, origin=None, lamport=None, propagate=True, consensus=False):
        with self.lock:
            if event_id and self.db.execute("SELECT 1 FROM events WHERE id=?", (event_id,)).fetchone(): return False
            if lamport is None: self.clock += 1; lamport = self.clock
            else: self.clock = max(self.clock, int(lamport)) + 1
            event = {"id": event_id or uuid.uuid4().hex, "kind": kind, "payload": payload, "origin": origin or self.node_id, "lamport": int(lamport), "version": int(version or 0), "ts": time.time()}
            with self.db: self.db.execute("INSERT INTO events(id,kind,payload,origin,ts,lamport,version) VALUES (?,?,?,?,?,?,?)", (event["id"], kind, json.dumps(payload), event["origin"], event["ts"], event["lamport"], event["version"]))
            self._apply(kind, payload, event["origin"], event["version"])
        if propagate: threading.Thread(target=self.gossip, args=(event,), daemon=True).start()
        return event

    def commit_event(self, kind, payload, version=0):
        if not self.raft: return self._event(kind, payload, version, propagate=True), None
        command = {"id": uuid.uuid4().hex, "kind": kind, "payload": payload, "version": version, "origin": self.node_id}
        result = self.raft.propose(command)
        if result.get("ok"): return self.db.execute("SELECT id,kind,payload,origin,ts,lamport,version FROM events WHERE id=?", (command["id"],)).fetchone(), result
        if result.get("leader") and result["leader"] in self.peers:
            import urllib.request
            try:
                req = urllib.request.Request(self.peers[result["leader"]] + "/internal/raft/propose", data=json.dumps(command).encode(), headers={"Content-Type": "application/json"}); response = json.load(urllib.request.urlopen(req, timeout=2)); return None, response
            except Exception: pass
        return None, result

    def receive(self, event): return self._event(event["kind"], event["payload"], event.get("version", 0), event["id"], event.get("origin"), event.get("lamport"), False)
    def gossip(self, event):
        import urllib.request
        for base in self.peers.values():
            try:
                req = urllib.request.Request(base + "/internal/events", data=json.dumps(event).encode(), headers={"Content-Type": "application/json"}); urllib.request.urlopen(req, timeout=.7).read()
            except Exception: pass

    def leader(self):
        alive = [sid for sid, state in self.membership.items() if state["alive"]]
        return max(alive, key=lambda sid: (self.config["nodes"][sid].get("priority", 0), sid)) if alive else None
    def refresh_leader(self):
        selected = self.leader()
        if selected != self.current_leader:
            self.leader_term += 1; self.current_leader = selected
            self._event("leader.changed", {"leader": selected, "term": self.leader_term}, version=self.leader_term)
        return selected
    def mark_peer_failure(self, peer_id):
        state = self.membership[peer_id]; state["misses"] += 1
        if state["misses"] >= FAILURE_THRESHOLD and state["alive"]:
            state["alive"] = False; self._event("failure.detected", {"node": peer_id, "detected_by": self.node_id, "misses": state["misses"]}); self.refresh_leader()
    def mark_peer_recovered(self, peer_id):
        was_alive = self.membership[peer_id]["alive"]; self.membership[peer_id] = {"alive": True, "misses": 0, "last_seen": time.time()}
        if not was_alive: self._event("failure.recovered", {"node": peer_id, "detected_by": self.node_id})
        self.refresh_leader()
    def sync_peer(self, peer_id):
        import urllib.request
        try:
            remote = json.load(urllib.request.urlopen(self.peers[peer_id] + "/internal/events", timeout=.8))
            for event in remote: self.receive(event)
            return True
        except Exception: return False
    def heartbeat_loop(self):
        import urllib.request
        while True:
            for peer_id, base in self.peers.items():
                try:
                    urllib.request.urlopen(base + "/health?shallow=1", timeout=.7).read()
                    self.mark_peer_recovered(peer_id); self.sync_peer(peer_id)
                except Exception:
                    self.mark_peer_failure(peer_id)
            time.sleep(HEARTBEAT_SECONDS)
    def health(self):
        return {"node": self.node_id, "status": "ok", "leader": self.raft.leader_id if self.raft else (self.current_leader or self.leader()), "leader_term": self.leader_term, "raft": self.raft.status() if self.raft else None, "membership": self.membership, "failure_alerts": self.failure_alerts[-20:], "events": self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], "lamport": self.clock, "batcher": self.batcher.metrics}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def _json(self, status, body):
        self.send_response(status); self.send_header("Content-Type", "application/json"); self.send_header("Access-Control-Allow-Origin", "*"); self.send_header("Access-Control-Allow-Headers", "Content-Type"); self.end_headers(); self.wfile.write(json.dumps(body).encode())
    def _body(self): return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0) or b"{}")
    def do_OPTIONS(self): self._json(204, {})
    def do_GET(self):
        p = urlparse(self.path); path = p.path; n = self.server.node
        if path == "/health": return self._json(200, {"node": n.node_id, "status": "ok"} if parse_qs(p.query).get("shallow") == ["1"] else n.health())
        if path == "/leader": return self._json(200, {"leader": n.current_leader or n.leader(), "term": n.leader_term, "membership": n.membership})
        if path == "/events": return self._json(200, [dict(x) for x in n.db.execute("SELECT id,kind,payload,origin,lamport,version,ts FROM events ORDER BY lamport,origin,id")])
        if path == "/profiles": return self._json(200, {"count": len(n.profiles), "profiles": list(n.profiles.values())[:20]})
        if path == "/listings":
            query = parse_qs(p.query); listings = list(n.listings.values())
            if query.get("location"): listings = [x for x in listings if x.get("location") == query["location"][0]]
            if query.get("status"): listings = [x for x in listings if x.get("status") == query["status"][0]]
            if query.get("max_rent"):
                listings = [x for x in listings if float(x.get("monthly_rent", 0)) <= float(query["max_rent"][0])]
            return self._json(200, listings)
        if path == "/scoring-config": return self._json(200, n.scoring_config)
        if path == "/raft/status": return self._json(200, n.raft.status() if n.raft else {"enabled": False})
        if path == "/reservations": return self._json(200, list(n.reservations.values()))
        if path.startswith("/notifications/"): return self._json(200, [x for x in n.notifications if str(x.get("user_id")) == path.rsplit("/", 1)[-1]])
        if path.startswith("/recommendations/"):
            uid = path.rsplit("/", 1)[-1]; profile = n.profiles.get(uid)
            if not profile: return self._json(404, {"error": "unknown profile"})
            return self._json(200, {"user_id": uid, "matches": n.batcher.submit(profile, list(n.profiles.values()), int(parse_qs(p.query).get("limit", [10])[0]))})
        if path == "/internal/events":
            rows = n.db.execute("SELECT id,kind,payload,origin,lamport,version,ts FROM events")
            return self._json(200, [{**dict(row), "payload": json.loads(row["payload"])} for row in rows])
        return self._json(404, {"error": "not found"})
    def do_POST(self):
        p = urlparse(self.path); n = self.server.node; body = self._body()
        if p.path == "/internal/raft/request-vote": return self._json(200, n.raft.request_vote(body))
        if p.path == "/internal/raft/append-entries": return self._json(200, n.raft.append_entries(body))
        if p.path == "/internal/raft/propose": return self._json(200, n.raft.propose(body))
        if p.path == "/internal/events": n.receive(body); return self._json(200, {"status": "accepted"})
        if p.path == "/profiles":
            uid = str(body.get("user_id", ""))
            if not uid: return self._json(400, {"error": "user_id required"})
            existing = n.profiles.get(uid, {}); expected = body.pop("expected_version", existing.get("profile_version", 0)); version = int(existing.get("profile_version", 0)) + 1
            if int(expected) != int(existing.get("profile_version", 0)): return self._json(409, {"error": "version conflict", "current_version": existing.get("profile_version", 0)})
            _, result = n.commit_event("profile.updated", body, version)
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(201, body)
        if p.path == "/listings":
            body.setdefault("listing_id", uuid.uuid4().hex); body.setdefault("status", "available"); _, result = n.commit_event("listing.created", body, 1)
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(201, body)
        if p.path == "/subscriptions":
            if "user_id" not in body or "event_kind" not in body: return self._json(400, {"error": "user_id and event_kind required"})
            _, result = n.commit_event("subscription.created", body)
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(201, body)
        if p.path == "/reservations":
            lid = str(body.get("listing_id", "")); uid = str(body.get("user_id", ""))
            if not lid or not uid: return self._json(400, {"error": "listing_id and user_id required"})
            if lid not in n.listings: return self._json(404, {"error": "unknown listing"})
            event = {"reservation_id": uuid.uuid4().hex, "listing_id": lid, "user_id": uid}; _, result = n.commit_event("reservation.requested", event, version=time.time_ns())
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(201, event)
        if p.path == "/recommendations":
            uid = str(body.get("user_id")); profile = n.profiles.get(uid)
            if not profile: return self._json(404, {"error": "unknown profile"})
            result = {"user_id": uid, "event_kind": "recommendation.generated", "matches": n.batcher.submit(profile, list(n.profiles.values()), int(body.get("limit", 10)))}; _, commit = n.commit_event("notification.sent", result)
            if commit and not commit.get("ok", True): return self._json(503, commit)
            return self._json(200, result)
        if p.path == "/scoring-config":
            expected = int(body.get("expected_version", n.scoring_config["version"])); current = n.scoring_config["version"]
            if expected != current: return self._json(409, {"error": "config version conflict", "current_version": current})
            _, result = n.commit_event("scoring.config.updated", {"weights": body.get("weights", {})}, version=current + 1)
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(200, n.scoring_config)
        return self._json(404, {"error": "not found"})

    def do_PUT(self):
        p = urlparse(self.path); n = self.server.node; body = self._body()
        if p.path.startswith("/listings/"):
            lid = p.path.rsplit("/", 1)[-1]; current = n.listings.get(lid)
            if not current: return self._json(404, {"error": "unknown listing"})
            expected = int(body.pop("expected_version", current.get("listing_version", 1)))
            if expected != int(current.get("listing_version", 1)): return self._json(409, {"error": "listing version conflict", "current_version": current.get("listing_version", 1)})
            updated = dict(current); updated.update(body); updated["listing_id"] = lid; _, result = n.commit_event("listing.updated", updated, version=expected + 1)
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(200, updated)
        return self._json(404, {"error": "not found"})

    def do_DELETE(self):
        p = urlparse(self.path); n = self.server.node
        if p.path.startswith("/reservations/"):
            lid = p.path.rsplit("/", 1)[-1]
            if lid not in n.reservations: return self._json(404, {"error": "unknown reservation"})
            _, result = n.commit_event("reservation.cancelled", {"listing_id": lid})
            if result and not result.get("ok", True): return self._json(503, result)
            return self._json(200, {"listing_id": lid, "cancelled": True})
        return self._json(404, {"error": "not found"})

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--node", required=True); ap.add_argument("--config", default="config.json"); ap.add_argument("--db", default=None); args = ap.parse_args()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); config_path = args.config if os.path.isabs(args.config) else os.path.join(root, args.config); config = json.load(open(config_path)); spec = config["nodes"][args.node]
    node = Node(args.node, config, args.db or os.path.join(root, "ml", "data", args.node + ".db"), os.path.join(root, "ml", "data", "processed", "roommate_clean.csv")); node.peers = {k: "http://%s:%s" % (v.get("advertise_host", v["host"]), v["port"]) for k, v in config["nodes"].items() if k != args.node}; node.attach_raft(); threading.Thread(target=node.heartbeat_loop, daemon=True).start()
    server = ThreadingHTTPServer((spec["host"], spec["port"]), Handler); server.node = node; print(json.dumps({"node": args.node, "port": spec["port"], "leader": node.leader()}), flush=True); server.serve_forever()
if __name__ == "__main__": main()
