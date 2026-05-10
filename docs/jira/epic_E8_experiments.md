# BSIM-E8 — Experiment Runner

---

## BSIM-37 — Panic threshold sweep harness

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-34

**Description:**
Implement the experiment runner that sweeps the panic threshold across a configured
range, running both schedulers at each value against the same event list, and
collating all results.

**Acceptance Criteria:**
- `run_experiment(event_list_path, panic_threshold_values, base_cfg, registry, output_dir)`
  runs Batch + K8S for each threshold value
- Each run's scorecard JSON saved to `output_dir/<scheduler>/threshold_<N>/scorecard.json`
- Collated results saved to `output_dir/collated.json`
- Progress displayed via rich progress bar
- Same event list used for all runs (fair comparison)

---

## BSIM-38 — Pareto frontier builder

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-37

**Description:**
From collated experiment results, identify Pareto-optimal configurations (threshold
values where no other value is strictly better on both cost AND mean wait time).

**Acceptance Criteria:**
- `build_pareto_frontier(collated, scheduler)` returns list of non-dominated points
- Each point tagged with `pareto_dominated: bool`
- Frontier saved to `output_dir/pareto_frontiers.json`
- Used as input data for Chart 1 (cost vs wait scatter)

---

## BSIM-39 — Meta-effect detector

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-37

**Description:**
Test the meta-effect hypothesis: that beyond some threshold value, increasing wait
tolerance begins to worsen cost efficiency by blocking node scale-down (long-running
jobs keep nodes alive, preventing shutdown).

**Acceptance Criteria:**
- `detect_meta_effect(collated)` scans (threshold, cost) pairs per scheduler
- Reports first threshold where cost increases from previous step (inflection point)
- Results saved to `output_dir/meta_effect.json`
- Result includes `detected: bool` and `inflection_threshold_s` per scheduler

---

## BSIM-40 — Reference centroid config and canonical workload

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-10

**Description:**
Define the four reference centroids representing the real-world workload population.
These become the canonical experiment input used to produce all published results.

**Centroids:**
- centroid_a: Small/Short — low download, modest RAM, 2-stage parallel (24/hr)
- centroid_b: Memory-Heavy — large download, very high RAM peak (15/hr)
- centroid_c: Compute-Heavy — long multi-stage workhorse, high thread count (15/hr)
- centroid_d: Balanced — moderate on all dimensions (6/hr)

**Acceptance Criteria:**
- Four centroid configs in `configs/reference_centroids.yaml`
- Arrival rates produce mix: ~40% A, ~25% B, ~25% C, ~10% D
- 4-hour simulated window produces ~242 jobs (seed=42)
- Workload saved to `workloads/reference_4h.json`

---

## BSIM-41 — Full experiment run and results validation

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-38, BSIM-39, BSIM-40

**Description:**
Execute the complete experiment sweep against the reference event list and validate
results are plausible and the central hypothesis is testable.

**Reference run results (seed=42, 4-hour window, 242 jobs):**

| Scheduler | Threshold | Cost (USD) | Mean Wait (s) | Crashes |
|-----------|-----------|-----------|--------------|---------|
| Batch     | 300s      | $402.61   | 390          | 0       |
| K8S       | 300s      | $397.95   | 390          | 0       |

K8S saves ~1.1% on this run. Savings expected to grow with workload density.

**Acceptance Criteria:**
- Sweep across 7 panic threshold values (60, 180, 300, 600, 900, 1800, 3600s)
- K8S cost ≤ Batch cost across all threshold values (hypothesis directionally confirmed)
- Pareto frontier charts generated
- Meta-effect analysis report generated
- All results committed to `results/reference_run/`
