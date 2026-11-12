# Fault-Tolerant Student Housing and Roommate Recommendation Platform

This directory contains the implementation for the CS249 project proposal. It is a local, dependency-free Python prototype of an event-driven platform for roommate matching and housing listings.

## Implemented

- CSV preprocessing and validation for the downloaded roommate-preference dataset.
- Explainable compatibility scoring with location, budget, lifestyle, study, food, pets, and interests features.
- Event-driven service for profiles, listings, recommendations, subscriptions, notifications, and event history.
- SQLite-backed durable state and idempotent event application.
- Lamport clocks, version checks, anti-entropy event recovery, event de-duplication, heartbeat membership, and deterministic leader election.
- A dependency-free Raft replicated log with terms, voting, AppendEntries, majority commit, persistent log/meta tables, and leader forwarding.
- Optimistic profile concurrency, deterministic reservation conflict resolution, subscriptions, and notification delivery records.
- A three-node local configuration and a repeatable evaluation script.

Run `python3 -m unittest discover -s ds/tests -p 'test_*.py' -v` for distributed-structure tests, `python3 ml/benchmark.py` for scoring throughput, and `python3 ds/fault_test.py` for the failure-detection settings.
Run `python3 ds/evaluate.py` for the combined scoring, batching, replication,
failure, leader, and consistency evaluation.
Run `python3 ds/process_fault_test.py` for a real multi-process leader-failure
test and `python3 ds/metrics.py` to generate latency, scalability, and recovery
graphs under `results/`.

The proposal's advanced micro-batching, replicated scoring-configuration versioning, Docker deployment, and dashboard remain stretch goals.

Docker Compose deployment is now provided in `deployment/compose.yaml`; see
`deployment/DEPLOYMENT.md` for local and EC2 instructions.

