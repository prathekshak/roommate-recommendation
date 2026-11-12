#!/usr/bin/env python3
"""Report the failure-detection settings used by the local cluster."""
import json, os
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); cfg = json.load(open(os.path.join(root, "config.json"), encoding="utf-8"))
print(json.dumps({"nodes": list(cfg["nodes"]), "heartbeat_interval_seconds": 2, "failure_threshold": 3, "expected_detection_upper_bound_seconds": 6}, indent=2))
print("To exercise it: start ./run.sh, stop one node, and query /health after 6 seconds.")
