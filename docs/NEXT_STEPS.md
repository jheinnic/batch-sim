# NEXT_STEPS.md — Handoff document for Claude Code

This file captures the current implementation state so Claude Code can
continue without reconstructing context from commit history.

---

## Current state of the repo (as of this document)

The local repo at `~/Git/batch-sim` is behind the Claude session repo by
approximately 14 commits. The key changes not yet on origin/main are listed
below. Apply them in order.

### How to sync

The changes are in three categories:
1. Files that exist on origin but need updating
2. New files that need creating
3. Config changes (already on origin via your pushes)

Run `git log --oneline origin/main..HEAD` to see the delta once you have
the updated files.

---

## Critical bug fixed (apply immediately)

**File: `batch_sim/generator/sampler.py`**

`perturbed_threads` was being Pareto-multiplied, causing declared vCPU to
balloon to 3-4x declared thread count. This made every node host exactly
1 job regardless of RAM or vCPU capacity.

Fix: replace `int(np.clip(round(t * pm()), 1, 64))` with `list(centroid.workhorse_hard_vcpu)`.

---

## Schema changes

**`batch_sim/core/schemas.py`** — major changes:
- `workhorse_thread_counts` field REMOVED entirely (was a duplicate of `workhorse_hard_vcpu`)
- `workhorse_hard_vcpu: list[int] | None` — canonical thread count declaration
- `workhorse_soft_vcpu: list[int] | None` — new: minimum vCPU guarantee per stage
- `burst_size_min: int = 1` — new: jobs per burst event (min)
- `burst_size_max: int = 1` — new: jobs per burst event (max)
- `cool_off_seconds: float = 0.0` — new: extra runtime after horizon for in-flight jobs
- `pareto_multiplier_min: float = 0.25` — new: per-centroid Pareto clamp
- `pareto_multiplier_max: float = 4.0` — new: per-centroid Pareto clamp
- `workhorse_io_wait_per_stage: list[float] | None` — new: per-stage I/O wait
- Validator: rejects `workhorse_hard_vcpu` and `workhorse_thread_counts` with different values

**`batch_sim/generator/job_spec.py`**:
- `build_phase_profile()` parameter renamed `workhorse_thread_counts` → `workhorse_hard_vcpu`
- `PhaseProfile.workhorse_declared_vcpu` = max(workhorse_hard_vcpu) unchanged
- `JobSpec` has two new fields: `soft_cpu: int = 0`, `hard_cpu: int = 0`

**`batch_sim/generator/sampler.py`**:
- Reads `centroid.workhorse_hard_vcpu` (not thread_counts)
- Computes `soft_cpu = max(workhorse_soft_vcpu)` if present, else `declared_vcpu`
- Computes `hard_cpu = max(workhorse_hard_vcpu)` if present, else `soft_cpu`
- Per-stage io_wait support via `workhorse_io_wait_per_stage`
- Per-centroid Pareto clamp via `pareto_multiplier_min/max`
- Burst arrivals: N jobs per Poisson event, N ~ Uniform[burst_size_min, max]

**`batch_sim/generator/event_list.py`**:
- `JobArrivalEvent` has new fields: `soft_cpu: int = 0`, `hard_cpu: int = 0`
- `to_job_spec()` carries `soft_cpu` and `hard_cpu` through
- `_event_from_job()` copies them from the sampled job
- Metadata now includes `cool_off_seconds` and `burst_params`

**`batch_sim/core/engine.py`**:
- `SimulationEngine.run()` accepts `cool_off_seconds: float = 0.0`
- `RunningJobSlot` has new field `soft_limit_ram_gb: float = 0.0`
- `NodeModel` has new field `spike_headroom_gb_at_launch: float = 0.0`
- `NodeModel.add_job()` accepts `soft_limit_gb=0.0`

---

## New scheduler files

**`batch_sim/scheduler/cpu_boost_solver.py`** — BSIM-70
Option 2 CPU boost solver: greedy, non-iterative, sorts jobs by io_wait
ascending, withholds returned cycles from redistribution.
```python
from batch_sim.scheduler.cpu_boost_solver import JobCPUState, solve_cpu_boost
```

**`batch_sim/scheduler/cpu_boost_integration.py`** — BSIM-71
Wires the boost solver into phase transitions; emits CPU_WASTE events.
NOT YET called from engine.py — this is the remaining wiring work.

**`batch_sim/scheduler/queue_router.py`** — BSIM-53
Advantage ratio formula: `(M - M²/C) / S`
Queue assignment for hybrid OKD+Batch routing.

**`batch_sim/scheduler/burst_pool.py`** — BSIM-55
`NodeBurstPool`: RAM-aware burst coordination replacing fixed-permit semaphore.

**`batch_sim/scheduler/k8s_plus_scheduler.py`** — BSIM-50
K8S+ with per-node burst pool (replaces semaphore).

**`batch_sim/scheduler/k8s_plus_two_queue.py`** — BSIM-54
Two-queue K8S+ with advantage-ratio routing.

**`batch_sim/experiment_hybrid.py`**
Hybrid OKD Q1 + Batch Q2 router and sweep runner.

---

## Updated existing files

**`batch_sim/experiment_runner.py`**:
- `run_one()` accepts `return_metrics=False`; when True returns `(Scorecard, MetricsCollector)`
- Passes `cool_off_seconds` from event list metadata to `engine.run()`

**`batch_sim/__main__.py`** (simulate command):
- Calls `run_one(..., return_metrics=True)`
- Saves event log to `<output>_events.json` alongside scorecard

**`batch_sim/scheduler/batch_scheduler.py`**:
- Placement uses `job.soft_cpu` (falls back to `workhorse_declared_vcpu`)

**`batch_sim/scheduler/k8s_scheduler.py`**:
- Placement uses `job.soft_cpu` (falls back to `workhorse_declared_vcpu`)

**`batch_sim/metrics/collector.py`**:
- New event type: `CPU_WASTE`

**`scripts/generate_node_timelines.py`**:
- New `--event-log` argument: reads saved `*_events.json` instead of re-running
- Fallback (no `--event-log`): calls `run_one()` — same code path as simulate
- No longer constructs schedulers directly (was the source of the discrepancy)
- Chart layout: stacked (Gantt top, CPU/RAM bottom), shared horizontal time axis
- Left labels show `cpu:soft/hard` and `ram:limit`

---

## New documentation

**`docs/CPU_MODELING.md`** — bias audit and CPU model decisions
**`docs/jira/epic_E13_cpu_hard_soft_limits.md`** — BSIM-69 through BSIM-73

---

## What still needs implementing (E13 remainder)

**BSIM-71 wiring** — `cpu_boost_integration.py` exists but is not called from
`engine.py`. Need to call `run_cpu_boost_k8s()` / `run_cpu_boost_batch()` at
each `PHASE_TRANSITION` emit in `run_job_process()`.

**BSIM-73** — Presentation update with two-argument K8S case and Chart D
(CPU waste decomposition bar chart).

**K8S crash investigation** — K8S scheduler still showing 526 crashes on the
jch workload. Root cause: headroom formula produces near-zero
`effective_schedulable_gb` for some nodes, causing overloads. The
`cpu_boost_integration.py` wiring and correct `soft_cpu` placement should
reduce this, but needs verification after wiring is complete.

---

## Config changes (already on origin)

- `network_bandwidth_mbps: 10000` (was 500) across all configs
- `cool_off_seconds: 3600` in all configs
- `workhorse_hard_vcpu` and `workhorse_soft_vcpu` arrays in jch_centroids_v01.yaml
- `workhorse_hard_vcpu` replaces `workhorse_thread_counts` everywhere
- Instance registry thinned to ≥128GB only (jch_instance_registry.yaml)
- `idle_timeout_seconds: 120`, `panic_threshold_seconds: 1800` in scheduler_reference.yaml

---

## Verified working state

After applying all changes:
```
Batch (jch workload, seed=3816): $93.91  2098/2098 jobs  98 nodes  ~21 jobs/node
K8S   (jch workload, seed=3816): $88.94  2152 jobs  82 nodes  ~26 jobs/node
```

The K8S crash rate is a separate issue from the scheduling efficiency — the
bin-packing works correctly, but burst collisions are still occurring.
