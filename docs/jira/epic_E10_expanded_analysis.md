# BSIM-E10 — Expanded Analysis: Dual-Strategy Timelines, Resource Utilization, 24-Hour Model, K8S+ Semaphore Scheduler, Workload Rebalancing

---

## BSIM-45 — Bug fix: _try_schedule queue deadlock

**Type:** Bug | **Priority:** Critical | **Status:** Done

**Description:**
Scheduler broke out of the placement loop when the front-of-queue job's reserved
(warming) node was not yet ready, leaving all subsequent jobs unserved even when
their nodes were already available.

**Root cause:** `_try_schedule` called `break` on the first unplaceable entry
rather than scanning the full queue for any placeable job.

**Effect:** 148/150 nodes sat idle; $402 wasted cost for 2 completed jobs.
After fix: 28 nodes, 225 jobs, $36.99 Batch / $36.62 K8S, ~$0.16/job.

**Fix:** Both `BatchScheduler._try_schedule` and `K8SScheduler._try_schedule`
now scan the full priority-ordered queue and place the first job that fits any
ready node, restarting the scan after each successful placement.

**Acceptance Criteria:**
- 28 tests still pass
- Reference run: ≥220 jobs complete, cost <$50, panics <100

---

## BSIM-46 — Dual-strategy node timeline visualization

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-45

**Description:**
The existing node timeline visualization shows only the Batch representative node.
Add a side-by-side or tabbed view showing a representative node from each strategy
so the scheduling differences are directly comparable.

**Requirements:**
- Both timelines use the same time scale so visual comparison is meaningful
- Each timeline identifies the instance type, its hourly rate, and total node cost
- Phase color coding is identical between both panels (download/preprocess/workhorse/upload)
- K8S timeline shows any burst collision / crash-and-replay events if they occur on the selected node
- Integrate both panels into `docs/presentation.html` as a new slide between the
  current "Reliability" slide and "Recommendation"

**Acceptance Criteria:**
- Both scheduler timelines visible without scrolling off the slide
- Representative node chosen is the median-busy node (not best or worst case)
- HTML file self-contained (no external data file dependency)

---

## BSIM-47 — Resource utilization plots: % reserved RAM and % reserved CPU over time

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-45

**Description:**
For each scheduler, plot the percentage of provisioned RAM and provisioned CPU
that is actually reserved (allocated to jobs) at each simulated time step across
the full pool of running nodes.

**Metrics:**
- % RAM reserved = sum(job.allocated_ram across all nodes) / sum(node.physical_ram across all nodes)
- % CPU reserved = sum(job.allocated_vcpu across all nodes) / sum(node.physical_vcpu across all nodes)
- Computed at 60-second intervals; both schedulers overlaid on same axes

**Note on "reserved" vs "consumed":**
Batch reserves by peak declaration (conservative upper bound).
K8S reserves by soft limit (8% of peak). The chart should annotate this distinction.

**Acceptance Criteria:**
- Two subplots: RAM utilization and CPU utilization
- Both schedulers overlaid per subplot
- Y-axis 0–100%
- Horizontal reference line at 80% (typical target utilization)
- Added to visualization suite (BSIM-36 chart slot 7 and 8)

---

## BSIM-48 — Multi-instance-type experiment: hybrid fleet optimization

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-45

**Description:**
The current simulation allows each provisioning decision to pick from the full
instance menu (general, memory-optimized, compute-optimized). Extend the
experiment runner to sweep across fleet composition strategies:

**Strategies to test:**
1. **Cheapest-fit (current):** always pick the cheapest instance that fits the job
2. **Memory-dominant:** prefer r7i instances even for compute-heavy jobs
3. **Compute-dominant:** prefer c7i instances; only fall back to r7i when RAM requires it
4. **Hybrid by centroid class:** memory-heavy jobs → r7i, compute-heavy → c7i, others → m7i
   (note: scheduler cannot use centroid label; this strategy is approximated by
   provisioning thresholds: if peak_ram > 64GB → r7i, elif declared_vcpu > 8 → c7i, else m7i)

**Output:** cost comparison table across strategies and both schedulers.

**Acceptance Criteria:**
- `InstanceSelectionStrategy` enum added to SchedulerConfig
- `InstanceRegistry.select(job, strategy)` method implements the four strategies
- Experiment runner sweeps strategies × schedulers × 1 panic threshold
- Results table shows: total cost, jobs/node ratio, memory utilization %, CPU utilization %

---

## BSIM-49 — 24-hour simulation with realistic arrival rate variation

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-45

**Description:**
The reference run covers a 4-hour flat-rate window. A 24-hour simulation with
realistic diurnal arrival patterns (high daytime, low overnight) exposes the
cost impact of idle provisioning as job rates drop — nodes that were launched
during the busy period sit idle during the quiet period before timing out.

**Arrival model extension:**
- Add `arrival_rate_multiplier_by_hour: list[float]` to `SimulationConfig`
  (24 values, one per hour; base rate is scaled by the multiplier)
- Reference profile: peak multiplier = 1.0 at hours 9–17, trough = 0.1 at hours 0–6
- The Poisson lambda for each centroid is scaled by the current hour's multiplier

**Outputs:**
- Cost-over-time curve annotated with "busy period" and "quiet period" bands
- Node count over time showing scale-up and scale-down behavior
- Idle cost during quiet period as a fraction of total cost
- Comparison: does K8S scale down faster (fewer nodes to drain) during the quiet period?

**Acceptance Criteria:**
- `SimulationConfig.arrival_rate_multiplier_by_hour` optional field (defaults to flat 1.0)
- 24-hour reference run config committed to `configs/`
- Charts generated and committed to `results/24h_run/`

---

## BSIM-50 — K8S+ scheduler: DaemonSet semaphore/mutex for collision-free memory bursts

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-25 (K8S scheduler)

**Description:**
Model a third scheduling strategy: K8S with a node-local semaphore facility
(implemented in production as a Kubernetes DaemonSet sidecar). Before entering
Phase 2 (the peak-RAM preprocess phase), each job acquires a semaphore/mutex.
The node's DaemonSet determines the concurrency limit based on the tier-local MM:

**Concurrency limit derivation:**
- Let `headroom_gb = node.physical_ram - os_overhead - spike_headroom`
  (same as `effective_schedulable_gb`, i.e. `node.physical_ram - 0.92*MM - os_overhead`)
- `burst_capacity = headroom_gb / MM`  (how many full MM spikes headroom can absorb simultaneously)
- If `burst_capacity >= 2`: semaphore with permits = floor(burst_capacity)
- If `burst_capacity >= 1`: semaphore with permits = 1 (effectively a mutex)
- If `burst_capacity < 1`: no job of this size should be scheduled here (capacity error)

**Behavioral effect:**
- Jobs queue for the semaphore before entering Phase 2 (they block in PREPROCESS_WAITING state)
- No crash-and-replay occurs for memory collisions — they are prevented entirely
- Jobs may wait slightly longer (semaphore queue time) but complete on first attempt
- Semaphore wait time is tracked as a new metric: `semaphore_wait_s`

**Implementation:**
- `K8SPlusScheduler` extends `K8SScheduler`
- `NodeSemaphore` per-node object: `acquire(job_id)` / `release(job_id)` SimPy processes
- New event type: `SEMAPHORE_WAIT` with fields: job_id, node_id, wait_s
- New metric: pool and per-centroid semaphore wait time (max/min/mean/stddev)

**Acceptance Criteria:**
- K8S+ run on reference workload: zero crash events
- Semaphore wait time reported in scorecard
- compare_scorecards extended to show K8S vs K8S+
- K8S+ included in experiment sweep (3 schedulers × 7 thresholds)
- Integration test: two simultaneous Phase-2 jobs on same node → second waits, no crash

---

## BSIM-51 — Workload rebalancing: increase burst-spike-bearing job fraction

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-2 (config schema), BSIM-40 (reference centroids)

**Description:**
The current reference mix (42% small/short, 25% memory-heavy, 26% compute-heavy,
7% balanced) under-represents jobs with the short high-RAM spike characteristic
that is the central modeling concern. The "balanced" centroid in particular has
moderate RAM and is not a strong test of the preprocess spike behavior.

**Proposed rebalancing:**
- Increase centroid_b (Memory-Heavy) arrival rate: 15/hr → 25/hr (~35% of total)
- Increase centroid_c (Compute-Heavy) arrival rate: 15/hr → 20/hr (~28% of total)
- Reduce centroid_a (Small/Short) arrival rate: 24/hr → 15/hr (~21% of total)
- Reduce centroid_d (Balanced) arrival rate: 6/hr → 2/hr (~3% of total)
- Add centroid_e (Burst-Dominant): large download + extreme RAM spike + very short
  workhorse (spike is most of job time): arrival_rate=10/hr (~14% of total)

**centroid_e definition:**
```yaml
id: centroid_e
label: "Burst-Dominant"
description: "Short jobs where the RAM spike IS the job — preprocess dominates."
arrival_rate_per_hour: 10
pareto_alpha: 2.3
download_gb: 25.0
preprocess_memory_exponent_a: 1.8
preprocess_memory_exponent_b: 1.7
preprocess_duration_seconds: 90    # near the 120s cap — spike is long
workhorse_cpu_stages: [60, 15, 60, 10]
workhorse_thread_counts: [4, 4]
io_wait_fraction: 0.35
upload_gb: 1.0
```

**Acceptance Criteria:**
- New arrival rates committed to `configs/reference_centroids_v2.yaml`
- centroid_e defined and validated
- Full experiment sweep run against v2 workload; results in `results/v2_run/`
- Summary table comparing v1 and v2 results for all three schedulers

---

## BSIM-52 — Updated presentation incorporating BSIM-46 through BSIM-51

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-46, BSIM-47, BSIM-48, BSIM-49, BSIM-50, BSIM-51

**Description:**
Incorporate all new analysis results into `docs/presentation.html` and update
the executive summary.

**Slide additions/changes:**
- Slide 5 (cost result): update to corrected $37 figures
- New slide after slide 6: dual node timeline (Batch vs K8S side-by-side)
- New slide: RAM and CPU utilization % over time (both schedulers)
- New slide: 24-hour simulation — scale-up, quiet period idle cost, scale-down
- New slide: three-way comparison — Batch vs K8S vs K8S+ (semaphore)
- New slide: workload v2 results — does increased burst fraction change the winner?
- Slide 9 (recommendation): update to recommend K8S+ prototype specifically

**Acceptance Criteria:**
- `docs/presentation.html` updated; all charts use real simulation data
- `docs/executive_summary.md` updated with corrected cost figures and K8S+ recommendation
- Git tag `v0.2.0` created on the commit containing all BSIM-E10 deliverables
