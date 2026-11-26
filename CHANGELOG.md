# Changelog

## v1.1.1 - Complete implementation documentation

### Improved

- Replaced the short README with a full implementation guide covering the
  architecture, algorithms, event model, API, UI, deployment, evaluation, and
  known limitations.

## v1.1.0 - Reproducible completion

### Added

- Deterministic 10,000-profile dataset generation with seed 249.
- Clean-clone fallback when the original roommate CSV is unavailable.
- Data-pipeline regression coverage.
- Evaluation checks for event replication and de-duplication, notification delivery,
  reservation conflict resolution, micro-batching, consistency, and failure alerts.

### Improved

- `demo.py`, `ml/benchmark.py`, and `ds/evaluate.py` now bootstrap their required
  data automatically.
- Evaluation results are written to `results/evaluation_summary.json`.
- README, implementation notes, deployment guide, and demo guide now describe the
  actual reproducible workflow and distinguish the lightweight teaching console
  from a production marketplace.
- Generated raw/processed data is ignored so fresh clones remain small.

### Verification

- 10 unit tests pass.
- End-to-end evaluation passes.
- Process-level Raft leader-failure recovery passes.
- Benchmark, metrics generation, and Python compilation pass.
