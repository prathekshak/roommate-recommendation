# Implementation and Results

## Scope

The proposal's committed core covers basic services, an event-driven workflow,
distributed algorithms, controlled failure visibility, and evaluation. This
implementation is a local Python prototype designed to run independently on a
single machine.

## Implemented components

1. `ml/preprocess.py` validates required fields, removes duplicate/invalid
   rows, and writes clean CSV/JSON plus a summary report.
2. `ml/recommender.py` implements a deterministic compatibility score. It
   uses location, budget overlap, sleep schedule, study habit, food, smoking,
   drinking, pets, cleanliness, noise, and shared interests. Each result
   includes reasons rather than an opaque prediction.
3. `ds/service.py` exposes profile, listing, recommendation, health, and event
   endpoints. Events are persisted in SQLite and applied idempotently.
4. `ds/service.py` also gossips new events to the other configured nodes and
   checks peer health with bounded timeouts.
5. `config.json` and `run.sh` provide three independent local nodes.

6. Distributed-systems structures include Lamport clocks, idempotent event
   IDs, anti-entropy synchronization, membership heartbeats, deterministic
   leader election, optimistic version checks, subscription records,
   notification records, and deterministic reservation conflict resolution.

7. `ui/index.html`, `ui/style.css`, and `ui/app.js` provide a low-level browser
   client for the REST inputs and distributed-system observability endpoints.

8. The recommendation service now uses a configurable micro-batcher and worker
   pool. `GET/POST /scoring-config` provides versioned scoring configuration
   updates with optimistic conflict checks.

9. Listings support versioned updates and search filters. Reservations support
   deterministic conflict resolution and cancellation. Notifications are
   delivered only when a user's subscription matches the event type.

10. Failure detection emits replicated `failure.detected` and
    `failure.recovered` events, and leader changes carry a monotonically
    increasing local election term.

11. `ds/raft.py` provides the consensus path for client writes: persistent
    term/vote state, RequestVote, AppendEntries, leader forwarding, majority
    commit, and follower application of committed commands. The former
    priority election remains only as an anti-entropy fallback when Raft is not
    attached.

12. `ds/process_fault_test.py` starts real node processes on isolated ports,
    kills the elected leader, measures replacement-leader recovery, and writes
    `results/process_fault_results.json`. `ds/metrics.py` writes detailed
    latency/scalability metrics and SVG graphs.

## Dataset and preprocessing result

The preferred source is `ml/data/raw/roommate_dataset.csv`, a roommate
lifestyle-preference CSV with the required schema. A clean clone does not need
to download anything: when that file is absent, the preprocessor creates a
deterministic 10,000-row demo dataset using `ml/generate_dataset.py` (seed
249). Running the preprocessor creates `ml/data/processed/roommate_clean.csv`,
`roommate_clean.json`, and `summary.json`.

The observed source data has 25 locations, integer budgets, categorical
lifestyle preferences, and pipe-separated interests. The preprocessing step
keeps all 10,000 rows because they have valid identifiers and numeric budget
and preference fields.

## How to reproduce results

    cd roommate-recommendation
    python3 ml/preprocess.py
    python3 demo.py

The demo prints the number of processed profiles and the top five matches for
profile `100000`. Start `./run.sh` to reproduce the distributed API, gossip,
health, and event-log demonstration in `demo-guide.txt`.

Saved result artifacts are available in `results/evaluation_summary.json` and
`results/recommendations_100000.json`.

Run the unit tests with `python3 -m unittest discover -s ds/tests -p 'test_*.py' -v`.
Run `python3 ml/benchmark.py` for a repeatable scoring throughput measurement and
`python3 ds/fault_test.py` for the failure-detection configuration.
Run `python3 ds/evaluate.py` for the combined evaluation report. It also
checks event de-duplication, notification delivery, reservation conflict
resolution, replication, and micro-batching, then writes
`results/evaluation_summary.json`.

## Limitations and next steps

This is a research/demo prototype, not a production housing marketplace. It
does not claim that compatibility scores are ground-truth outcomes, and the
dataset is lifestyle data rather than verified housing listings. The browser
console is intentionally lightweight. The reservation and profile conflict
paths provide the core concurrency-control demonstration; the process-level
Raft test provides the leader-failure demonstration.
