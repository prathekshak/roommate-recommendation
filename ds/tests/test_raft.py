import os, sqlite3, sys, tempfile, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from raft import RaftNode

class RaftProtocolTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.db = sqlite3.connect(os.path.join(self.tmp.name, "raft.db")); self.db.row_factory = sqlite3.Row; self.applied = []
        self.raft = RaftNode("A", {}, self.db, self.applied.append)
    def tearDown(self): self.tmp.cleanup()
    def test_vote_is_term_scoped(self):
        first = self.raft.request_vote({"term": 1, "candidate_id": "B", "last_log_index": 0, "last_log_term": 0})
        second = self.raft.request_vote({"term": 1, "candidate_id": "C", "last_log_index": 0, "last_log_term": 0})
        self.assertTrue(first["vote_granted"]); self.assertFalse(second["vote_granted"])
    def test_append_entries_commits_and_applies(self):
        response = self.raft.append_entries({"term": 2, "leader_id": "B", "prev_log_index": 0, "prev_log_term": 0, "entries": [{"idx": 1, "term": 2, "command": {"kind": "listing.created"}}], "leader_commit": 1})
        self.assertTrue(response["success"]); self.assertEqual(self.raft.commit_index, 1); self.assertEqual(self.applied[0]["kind"], "listing.created")
    def test_single_node_majority_commit(self):
        self.raft.role = "leader"; self.raft.current_term = 1; result = self.raft.propose({"kind": "profile.updated"})
        self.assertTrue(result["ok"]); self.assertEqual(self.raft.commit_index, 1)

if __name__ == "__main__": unittest.main(verbosity=2)
