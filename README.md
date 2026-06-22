# batch-sim

Discrete-event simulation comparing the cost and service quality of **AWS Batch**
vs **OKD/K8S** for batch compute scheduling workloads.

## Structure

```
batch_sim/
  core/        SimPy environment, node state, overload detection, job execution
  generator/   Workload generator: centroid params, Pareto sampler, event list
  scheduler/   Batch and K8S scheduler implementations
  registry/    EC2 instance type definitions and cost model
  metrics/     Event collector, aggregators, scorecard renderer, charts
configs/       Reference centroid + instance registry + scheduler YAMLs
workloads/     Saved event lists (Stage 1 output)
results/       Simulation run outputs and charts
docs/jira/     44 Jira tickets across 9 epics
tests/         28-test pytest suite
```

## Quickstart

```bash
pip install -e ".[dev]"

# Stage 1: generate a workload
python -m batch_sim generate \
  --config configs/reference_centroids.yaml \
  --output workloads/reference_4h.json

# Stage 2: run experiment sweep (both schedulers, 7 panic thresholds)
python -m batch_sim experiment \
  --events workloads/reference_4h.json \
  --scheduler-config configs/scheduler_reference.yaml \
  --output results/reference_run \
  --thresholds "60,180,300,600,900,1800,3600"

# Compare two individual scorecard files
python -m batch_sim compare \
  --batch results/reference_run/batch/threshold_300/scorecard.json \
  --k8s   results/reference_run/k8s/threshold_300/scorecard.json

# Regenerate charts
python -m batch_sim plot --experiment-dir results/reference_run
```

## Reference run results (seed=42, 4-hour window, 242 jobs)

| Scheduler | Cost (USD) | Mean Wait (s) | Crashes |
|-----------|-----------|--------------|---------|
| AWS Batch | $402.61   | 390s         | 0       |
| OKD/K8S   | $397.95   | 390s         | 0       |

K8S saves ~1.1% on a moderate 4-hour window; savings grow with workload density.

## Modeling Decisions & Known Limitations

Before citing a number from this simulation to support a real decision, read:

- [docs/CPU_MODELING.md](docs/CPU_MODELING.md) — CPU scheduling model for Batch
  (CFS proportional shares) vs. K8S+ (hard/soft limits), with a bias-direction
  analysis of which side each simplification favors.
- [docs/NODE_LIFECYCLE_MODELING.md](docs/NODE_LIFECYCLE_MODELING.md) — the
  simulation's event-driven node disruption (drain/terminate) decisions are
  exact, where a real Karpenter-style controller's are sampled on a
  reconciliation cadence. This is a one-directional, unquantified distortion:
  real-world K8S+ node-hours/cost plausibly run higher than this simulation
  predicts, never lower.

## Jira Tickets

See [docs/jira/README.md](docs/jira/README.md) — 44 tickets across 9 epics.
