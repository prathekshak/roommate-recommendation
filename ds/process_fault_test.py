#!/usr/bin/env python3
"""Start real node processes, commit a write, stop the leader, and measure recovery."""
import json, os, subprocess, sys, tempfile, time, urllib.request
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get(url):
    with urllib.request.urlopen(url, timeout=1) as response: return json.load(response)
def post(url, body):
    request = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=2) as response: return json.load(response)
def wait_for(url, timeout=12):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try: return get(url)
        except Exception: time.sleep(.2)
    raise RuntimeError("Timed out waiting for " + url)

def main():
    with tempfile.TemporaryDirectory(prefix="cs249-raft-") as tmp:
        ports = {"A": 8311, "B": 8312, "C": 8313}; config = {"nodes": {sid: {"host": "127.0.0.1", "port": port, "priority": i + 1} for i, (sid, port) in enumerate(ports.items())}}
        config_path = os.path.join(tmp, "config.json"); open(config_path, "w").write(json.dumps(config))
        processes = {}
        try:
            for sid in ports:
                log = open(os.path.join(tmp, sid + ".log"), "w")
                processes[sid] = subprocess.Popen([sys.executable, os.path.join(ROOT, "ds", "service.py"), "--node", sid, "--config", config_path, "--db", os.path.join(tmp, sid + ".db")], stdout=log, stderr=log)
            statuses = {sid: wait_for("http://127.0.0.1:%s/raft/status" % port) for sid, port in ports.items()}
            deadline = time.time() + 15; leader = None
            while time.time() < deadline and leader is None:
                for sid, port in ports.items():
                    try:
                        status = get("http://127.0.0.1:%s/raft/status" % port)
                        if status.get("role") == "leader": leader = sid; break
                    except Exception: pass
                if leader is None: time.sleep(.2)
            if leader is None:
                for sid in ports: print("---", sid, "---"); print(open(os.path.join(tmp, sid + ".log")).read())
                raise RuntimeError("Raft election did not converge")
            listing = post("http://127.0.0.1:%s/listings" % ports[leader], {"title": "Committed before failure", "location": "Kathmandu", "monthly_rent": 10000}); listing_id = listing["listing_id"]
            start = time.time(); processes[leader].terminate(); processes[leader].wait(timeout=3)
            survivors = [sid for sid in ports if sid != leader]; new_leader = None
            while time.time() - start < 12:
                for survivor in survivors:
                    try:
                        status = get("http://127.0.0.1:%s/raft/status" % ports[survivor])
                        if status.get("role") == "leader": new_leader = survivor; break
                    except Exception: pass
                if new_leader: break
                time.sleep(.2)
            recovered = False
            if new_leader:
                recovered = any(item.get("listing_id") == listing_id for item in get("http://127.0.0.1:%s/listings" % ports[new_leader]))
            result = {"initial_leader": leader, "new_leader": new_leader, "recovery_seconds": round(time.time() - start, 3), "committed_write_recovered": recovered, "passed": new_leader is not None and recovered}
            os.makedirs(os.path.join(ROOT, "results"), exist_ok=True); open(os.path.join(ROOT, "results", "process_fault_results.json"), "w").write(json.dumps(result, indent=2)); print(json.dumps(result, indent=2))
            if not result["passed"]: raise SystemExit(1)
        finally:
            for process in processes.values():
                if process.poll() is None: process.terminate()
            for process in processes.values():
                try: process.wait(timeout=3)
                except Exception: process.kill()

if __name__ == "__main__": main()
