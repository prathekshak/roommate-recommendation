# Fault-Tolerant Student Housing and Roommate Recommendation Platform

CS249 project implementation for a distributed student-housing and roommate-
recommendation platform. It combines an explainable compatibility scorer with
an event-driven service, replicated state, failure detection, concurrency
control, and a small browser console.

This is a research/demo prototype, not a production housing marketplace. The
compatibility score is a transparent preference-similarity score, not a claim
that the system predicts real-world roommate outcomes.

## What the project implements

### 1. Reproducible data pipeline

- ml/generate_dataset.py creates a deterministic 10,000-profile demo dataset
  with seed 249.
- ml/preprocess.py validates the required schema, removes duplicate IDs, skips
  malformed numeric rows, and writes cleaned CSV, JSON, and summary files.
- If ml/data/raw/roommate_dataset.csv exists, it is used instead of generated
  demo data, so the original public dataset can be supplied without code changes.
- Generated data is ignored by Git so a clone stays small and reproducible.

### 2. Explainable roommate recommendations

ml/recommender.py ranks candidate profiles using a deterministic weighted score.
It considers location, overlapping budget ranges, sleep schedule, study habit,
food, smoking, drinking, pets, cleanliness, noise level, and shared interests.

Every result includes a numeric score, human-readable reasons, and shared
interests. Gender preference is applied as a hard filter before scoring. Results
are sorted deterministically by score and user ID.

### 3. Event-driven service

ds/service.py provides each application node. State changes are represented as
events with unique IDs, origin nodes, Lamport timestamps, versions, payloads,
and event kinds. Events are stored durably in SQLite and replayed at startup.

Supported state includes profiles, listings, listing updates, recommendations,
subscriptions, notification delivery records, reservations, cancellations,
scoring-configuration versions, failure alerts, and leader-change records.

Repeated event IDs are ignored, so receiving an event twice does not apply the
state change twice.

### 4. Distributed coordination

The platform demonstrates Lamport clocks, optimistic version checks,
deterministic reservation conflict resolution, heartbeat membership, gossip
dissemination, anti-entropy synchronization, subscription filtering, and
notification delivery tracking.

### 5. Raft replicated log

ds/raft.py implements persistent term/vote state, RequestVote, AppendEntries,
leader heartbeats, majority commits, persistent log/meta tables, leader
forwarding, and application of committed commands. The three-node configuration
can elect a replacement leader after the current leader stops while preserving a
committed listing write.

### 6. Recommendation micro-batching

RecommendationBatcher in ds/service.py collects requests for a short configurable
window and processes them with worker threads. Batch counts, request counts, and
maximum batch size are exposed in health and evaluation data.

### 7. Versioned scoring configuration

Scoring weights are stored as a versioned replicated record. Updates use an
expected-version check, so stale concurrent updates are rejected. The active
configuration is available through GET /scoring-config.

### 8. Browser console and deployment

The ui/ directory provides a lightweight console for health/Raft status, event
logs, listings, recommendations, profiles, subscriptions, and reservations.
deployment/compose.yaml runs three API nodes and the frontend with persistent
Docker volumes. Deployment and EC2 notes are in deployment/DEPLOYMENT.md.

## Repository layout

~~~text
ml/                     Data generation, preprocessing, and scoring
ds/service.py            HTTP API, events, state, gossip, and batching
ds/raft.py               Dependency-free Raft implementation
ds/evaluate.py           End-to-end core evaluation
ds/process_fault_test.py Real multi-process leader-failure test
ds/metrics.py            Latency, scalability, and recovery graphs
ds/tests/                Distributed, Raft, and data-pipeline tests
ui/                     Browser console
deployment/              Docker Compose deployment
results/                 Generated evaluation artifacts
demo.py                 Preprocessing and recommendation smoke demo
run.sh                  Three-node local launcher
~~~

## Requirements

- Python 3.9 or newer.
- No third-party Python packages for the local prototype.
- Docker Desktop or Docker Engine only for container deployment.

## Quick start

From the repository root:

~~~bash
python3 demo.py
~~~

This creates the demo dataset if necessary, preprocesses 10,000 profiles, and
prints the top five matches for profile 100000.

Start the three-node local API:

~~~bash
./run.sh
~~~

The nodes listen on ports 8201, 8202, and 8203. Start the browser console
separately:

~~~bash
python3 -m http.server 8080 --directory ui
~~~

Open http://127.0.0.1:8080.

## API reference

The API uses JSON.

Read endpoints:

- GET /health: node status, membership, alerts, event count, Lamport clock,
  and batch metrics.
- GET /health?shallow=1: lightweight heartbeat response.
- GET /leader: current visible leader and membership.
- GET /raft/status: Raft role, term, leader, commit index, and log length.
- GET /events: local durable event history.
- GET /profiles: profile count and a profile preview.
- GET /listings: listings, with optional location, status, and max_rent filters.
- GET /recommendations/{user_id}: ranked matches with an optional limit.
- GET /notifications/{user_id}: notification records for a user.
- GET /reservations: current reservation winners.
- GET /scoring-config: active scoring weights and version.

Write endpoints:

- POST /profiles: create/update a profile with optimistic version checking.
- POST /listings: create a listing.
- PUT /listings/{listing_id}: update a listing with version checking.
- POST /subscriptions: subscribe a user to an event kind.
- POST /recommendations: generate and record a recommendation notification.
- POST /reservations: request a listing reservation.
- DELETE /reservations/{listing_id}: cancel the current reservation.
- POST /scoring-config: update scoring weights with expected_version.

Internal /internal/ endpoints support Raft RPCs and event exchange.

## Reproducible evaluation

Run the unit tests:

~~~bash
python3 -m unittest discover -s ds/tests -p 'test_*.py' -v
~~~

Run the end-to-end evaluation:

~~~bash
python3 ds/evaluate.py
~~~

It bootstraps the dataset and verifies 10,000 valid profiles, replicated and
de-duplicated events, notification delivery, deterministic reservation conflict
resolution, concurrent micro-batching, replicated listing consistency, and
failure-alert generation.

Run the scoring benchmark:

~~~bash
python3 ml/benchmark.py
~~~

Run the process-level Raft failure test:

~~~bash
python3 ds/process_fault_test.py
~~~

It starts three real API processes, commits a listing, stops the elected
leader, waits for a replacement, and checks that the committed listing survives.

Generate latency, scalability, and recovery graphs:

~~~bash
python3 ds/metrics.py
~~~

Generated artifacts include results/evaluation_summary.json,
results/evaluation_metrics.json, results/process_fault_results.json,
results/latency-scalability.svg, and results/raft-recovery.svg.

## Docker deployment

~~~bash
docker compose -f deployment/compose.yaml build
docker compose -f deployment/compose.yaml up -d
docker compose -f deployment/compose.yaml ps
~~~

API nodes use ports 8201-8203; the frontend uses port 8080. Stop the stack with:

~~~bash
docker compose -f deployment/compose.yaml down
~~~

Named volumes preserve SQLite state. Use down -v only when intentionally
deleting node databases.

## Version history

### v1.1.1

Expanded the README into a complete implementation, API, architecture,
deployment, evaluation, and limitations guide.

### v1.1.0

Added deterministic dataset bootstrapping, enhanced end-to-end evaluation,
data-pipeline tests, corrected documentation, and verified Raft recovery. See
CHANGELOG.md.

## Known limitations

- The demo dataset is synthetic when the original CSV is not supplied.
- Compatibility scores are heuristics, not validated roommate outcomes.
- The UI is a teaching console, not an authentication-enabled marketplace.
- SQLite and local HTTP are demonstration choices, not production-scale storage.
- Authentication, payment processing, lease verification, privacy controls, and
  production database operations are outside this prototype's scope.

## Academic use

This repository is intended for the CS249 project demonstration and evaluation.
Review the repository history and proposal materials for the academic scope.

