# BSIM-E22 — Retire the Advantage-Ratio Experiments

Removes the experimental two-queue / hybrid scheduling family (BSIM-53/54) whose
routing was driven by a computed *advantage ratio* heuristic. The tier-compatibility
model (E20) supersedes it with explicit, operator-declared placement that maps directly
to real K8s taints/tolerations — a more faithful and more controllable mechanism than an
invisible per-job ratio. The dead code is also what keeps `reproduce_all.sh` Run-03 and
Run-04 broken (Run-04 infinite-spins).

## What goes, what stays

**Removed:**
- `K8SPlusTwoQueueScheduler` (`k8s_plus_two_queue.py`) — advantage-ratio two-queue
  scheduler; never wired into the `SchedulerType` enum or `run_one`, only driven by
  `reproduce_all.sh`.
- `experiment_hybrid.py` (`run_hybrid_sweep`) — the Q1-instance × k hybrid sweep
  (Run-04), the source of the infinite spin.

**Kept (deliberately):**
- `queue_router.py` — decoupled from the schedulers (it imports only `core.schemas` and
  `registry`, nothing being deleted). Retained as an **analysis-only** utility: the
  advantage ratio remains a useful *descriptive* metric even though no scheduler routes
  by it. `inspect_workload.py`'s advantage-ratio report stays.
- `burst_pool.py` (`NodeBurstPool`) — **not** part of this retirement. It is the GB-aware
  Phase-2 burst-concurrency mechanism that is being *promoted* into the mainline K8S+
  scheduler under BSIM-122 (E20). It is complementary to the tier reservation model, not
  superseded by it.
- **Run-05 (utilization-charts reporting) and `generate_utilization_charts.py` are out
  of scope for this epic, full stop.** This epic only prunes Run-03/Run-04 — the dead
  *scheduler* code and its sweep — never any *reporting* logic. Run-05 is independently
  broken (a stale `cfg_sched.k8s_os_overhead_gb` reference from before BSIM-109's schema
  split) and worth keeping, but deciding its fate — repair, generalize, or drop — belongs
  to E23's salvage assessment (BSIM-118), not here.

This epic is pure deletion plus the `reproduce_all.sh` pruning needed to keep the tree
green; it has no behavioural effect on the three production schedulers, and it touches
no reporting/comparison-output logic.

Depends on: nothing (the removed code is already orphaned from `run_one`).
Related: BSIM-122 (E20) promotes `burst_pool`; E23 retires `reproduce_all.sh` entirely
and, per BSIM-118, assesses Run-05's utilization-charts framework for salvage rather
than this epic dropping it by default.

---

## BSIM-116 — Remove the K8S+ two-queue scheduler

**Type:** Task | **Priority:** Medium | **Status:** Done

**Description:**
Delete `batch_sim/scheduler/k8s_plus_two_queue.py` (`K8SPlusTwoQueueScheduler`) and the
Run-03 block in `reproduce_all.sh` that instantiates it. The scheduler is not in the
`SchedulerType` enum and is unreachable via `run_one`, so production paths are unaffected.

Confirm the kept utilities remain intact after removal: `queue_router` and
`inspect_workload` still import and run (they have no dependency on the deleted
scheduler), and `burst_pool` remains for BSIM-122.

**Acceptance Criteria:**
- `k8s_plus_two_queue.py` deleted; no remaining imports of `K8SPlusTwoQueueScheduler`
- Run-03 removed from `reproduce_all.sh`; the script has no dangling import of the deleted
  scheduler
- `queue_router.py` and `burst_pool.py` are untouched and still import cleanly
- `inspect_workload.py` still runs (advantage-ratio analysis intact)
- Full test suite green

---

## BSIM-117 — Remove the hybrid OKD+Batch sweep

**Type:** Task | **Priority:** Medium | **Status:** Done

**Description:**
Delete `batch_sim/experiment_hybrid.py` (`run_hybrid_sweep`) and the Run-04 block in
`reproduce_all.sh` that invokes it. Run-04 is the infinite-spin; the hybrid sweep is part
of the same advantage-ratio experiment family and is not referenced by any production
path. Remove any experiment-mode inputs that exist solely to parameterise it (e.g.
advantage-ratio `k` / Q1-instance sweep inputs), confirming none are shared with the
generic panic-threshold `run_experiment`.

**Acceptance Criteria:**
- `experiment_hybrid.py` deleted; no remaining imports of `run_hybrid_sweep`
- Run-04 removed from `reproduce_all.sh`; no dangling import
- Any config/CLI inputs unique to the hybrid sweep are removed; the generic
  `run_experiment` (panic sweep) is unaffected
- `queue_router.py` retained as analysis-only; `inspect_workload.py` still runs
- Full test suite green
