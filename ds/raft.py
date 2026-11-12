#!/usr/bin/env python3
"""Small dependency-free Raft implementation for the local CS249 cluster."""
import json, random, threading, time, urllib.request

class RaftNode:
    def __init__(self, node_id, peers, db, apply_command):
        self.node_id, self.peers, self.db, self.apply_command = node_id, peers, db, apply_command
        self.lock = threading.RLock(); self.role = "follower"; self.current_term = 0; self.voted_for = None; self.leader_id = None
        self.commit_index = 0; self.last_applied = 0; self.next_index = {}; self.match_index = {}; self.stopped = False
        with db:
            db.execute("CREATE TABLE IF NOT EXISTS raft_meta(key TEXT PRIMARY KEY, value TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS raft_log(idx INTEGER PRIMARY KEY, term INTEGER, command TEXT)")
        self._load(); self._reset_election()

    def _load(self):
        for row in self.db.execute("SELECT key,value FROM raft_meta"):
            if row["key"] == "term": self.current_term = int(row["value"])
            if row["key"] == "voted_for": self.voted_for = row["value"] or None
        self.commit_index = int(self._meta("commit_index", "0")); self.last_applied = int(self._meta("last_applied", "0"))
    def _meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM raft_meta WHERE key=?", (key,)).fetchone(); return row["value"] if row else default
    def _save(self):
        with self.db:
            for key, value in [("term", self.current_term), ("voted_for", self.voted_for or ""), ("commit_index", self.commit_index), ("last_applied", self.last_applied)]: self.db.execute("INSERT OR REPLACE INTO raft_meta VALUES (?,?)", (key, str(value)))
    def _reset_election(self): self.election_deadline = time.time() + random.uniform(1.5, 3.0)
    def _majority(self): return (len(self.peers) + 1) // 2 + 1
    def _last(self):
        row = self.db.execute("SELECT idx,term FROM raft_log ORDER BY idx DESC LIMIT 1").fetchone(); return (int(row["idx"]), int(row["term"])) if row else (0, 0)
    def _rpc(self, peer, path, body):
        try:
            req = urllib.request.Request(self.peers[peer] + path, data=json.dumps(body).encode(), headers={"Content-Type":"application/json"}); return json.load(urllib.request.urlopen(req, timeout=.8))
        except Exception: return None
    def start(self): threading.Thread(target=self._loop, daemon=True).start()
    def stop(self): self.stopped = True
    def _loop(self):
        while not self.stopped:
            if self.role == "leader": self._broadcast_heartbeat(); time.sleep(.35)
            elif time.time() >= self.election_deadline: self._start_election()
            else: time.sleep(.08)
    def _start_election(self):
        with self.lock:
            self.role = "candidate"; self.current_term += 1; self.voted_for = self.node_id; self.leader_id = None; self._reset_election(); self._save(); term = self.current_term; last_idx, last_term = self._last()
        votes = 1
        for peer in self.peers:
            response = self._rpc(peer, "/internal/raft/request-vote", {"term": term, "candidate_id": self.node_id, "last_log_index": last_idx, "last_log_term": last_term})
            if response and response.get("term", 0) > self.current_term: self._become_follower(response["term"]); return
            if response and response.get("vote_granted"): votes += 1
        if votes >= self._majority():
            with self.lock:
                self.role = "leader"; self.leader_id = self.node_id; self.next_index = {p: self._last()[0] + 1 for p in self.peers}; self.match_index = {p: 0 for p in self.peers}
    def _become_follower(self, term, leader=None):
        with self.lock:
            if term > self.current_term: self.current_term = term; self.voted_for = None
            self.role = "follower"; self.leader_id = leader; self._reset_election(); self._save()
    def _broadcast_heartbeat(self):
        for peer in self.peers: self._replicate(peer, None)
    def _replicate(self, peer, new_entry):
        last_idx, _ = self._last(); next_idx = self.next_index.get(peer, last_idx + 1); prev_idx = next_idx - 1; prev_row = self.db.execute("SELECT term FROM raft_log WHERE idx=?", (prev_idx,)).fetchone(); prev_term = int(prev_row["term"]) if prev_row else 0
        entries = []
        if new_entry is not None: entries = [new_entry]
        elif next_idx <= last_idx:
            for row in self.db.execute("SELECT idx,term,command FROM raft_log WHERE idx>=? ORDER BY idx", (next_idx,)): entries.append({"idx": row["idx"], "term": row["term"], "command": json.loads(row["command"])})
        response = self._rpc(peer, "/internal/raft/append-entries", {"term": self.current_term, "leader_id": self.node_id, "prev_log_index": prev_idx, "prev_log_term": prev_term, "entries": entries, "leader_commit": self.commit_index})
        if not response: return False
        if response.get("term", 0) > self.current_term: self._become_follower(response["term"]); return False
        if not response.get("success") and next_idx > 1:
            self.next_index[peer] = next_idx - 1
            return self._replicate(peer, new_entry)
        if response.get("success") and entries:
            self.match_index[peer] = entries[-1]["idx"]; self.next_index[peer] = entries[-1]["idx"] + 1
        return bool(response.get("success"))
    def propose(self, command):
        with self.lock:
            if self.role != "leader": return {"ok": False, "leader": self.leader_id, "term": self.current_term}
            idx = self._last()[0] + 1; entry = {"idx": idx, "term": self.current_term, "command": command}
            with self.db: self.db.execute("INSERT INTO raft_log VALUES (?,?,?)", (idx, self.current_term, json.dumps(command)))
        acknowledged = 1
        for peer in self.peers:
            if self._replicate(peer, entry): acknowledged += 1
        if acknowledged >= self._majority():
            with self.lock: self.commit_index = idx; self._apply_committed(); self._save()
            self._broadcast_heartbeat(); return {"ok": True, "index": idx, "term": self.current_term}
        return {"ok": False, "leader": self.node_id, "term": self.current_term, "error": "majority unavailable"}
    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1; row = self.db.execute("SELECT command FROM raft_log WHERE idx=?", (self.last_applied,)).fetchone()
            if row: self.apply_command(json.loads(row["command"]))
    def request_vote(self, request):
        with self.lock:
            term = int(request.get("term", 0))
            if term < self.current_term: return {"term": self.current_term, "vote_granted": False}
            if term > self.current_term: self._become_follower(term)
            last_idx, last_term = self._last(); up_to_date = (int(request.get("last_log_term", 0)), int(request.get("last_log_index", 0))) >= (last_term, last_idx)
            grant = (self.voted_for in (None, request.get("candidate_id"))) and up_to_date
            if grant: self.voted_for = request.get("candidate_id"); self._reset_election(); self._save()
            return {"term": self.current_term, "vote_granted": grant}
    def append_entries(self, request):
        with self.lock:
            term = int(request.get("term", 0))
            if term < self.current_term: return {"term": self.current_term, "success": False}
            self._become_follower(term, request.get("leader_id")); prev = int(request.get("prev_log_index", 0)); prev_row = self.db.execute("SELECT term FROM raft_log WHERE idx=?", (prev,)).fetchone()
            if prev and (not prev_row or int(prev_row["term"]) != int(request.get("prev_log_term", 0))): return {"term": self.current_term, "success": False}
            for entry in request.get("entries", []):
                old = self.db.execute("SELECT term FROM raft_log WHERE idx=?", (entry["idx"],)).fetchone()
                if old and int(old["term"]) != int(entry["term"]): self.db.execute("DELETE FROM raft_log WHERE idx>=?", (entry["idx"],))
                if not self.db.execute("SELECT 1 FROM raft_log WHERE idx=?", (entry["idx"],)).fetchone(): self.db.execute("INSERT INTO raft_log VALUES (?,?,?)", (entry["idx"], entry["term"], json.dumps(entry["command"])))
            self.commit_index = min(int(request.get("leader_commit", 0)), self._last()[0]); self._apply_committed(); self._save(); return {"term": self.current_term, "success": True}
    def status(self): return {"role": self.role, "term": self.current_term, "leader": self.leader_id, "commit_index": self.commit_index, "last_applied": self.last_applied, "log_length": self._last()[0]}
