# BSIM-E12 — Corrected Utilization Metrics: Three-Form Framework

Supersedes the utilization reporting in BSIM-47.
Implements the precise three-percentage, three-utilization-form framework
agreed in design review.

---

## BSIM-61 — Canonical metric definitions and OS overhead treatment

**Type:** Task | **Priority:** Highest | **Status:** To Do

**Description:**
Establish authoritative definitions for all capacity and utilization metrics.
These definitions govern all reporting from this point forward.

**Definitions:**

```
os_overhead_gb  = cfg.k8s_os_overhead_gb (K8S) or 0 (Batch, which does not deduct OS)

Allocated       = sum(node.ram_gb - os_overhead_gb  for all running nodes)
                  [net schedulable physical capacity; OS pre-subtracted and thereafter ignored]

Reserved        = for Batch: sum(job.preprocess_peak_ram_gb  for all placed jobs)
                  for K8S:   sum(job.preprocess_steady_ram_gb for all placed jobs)
                           + sum(node.spike_headroom_gb_at_launch for all running nodes)
                  [spike_headroom_gb_at_launch = 0.92 × tier_local_MM at node launch time;
                   fixed at launch, does not change as jobs arrive or depart]

Util₁ (Instantaneous Utilization)
                = sum(job.current_phase_ram_gb  for all running jobs)
                  [actual hardware consumption at this simulated moment]

Util₃ (Min Theoretical Utilization)
                = sum(min(job.current_phase_ram_gb, job.soft_limit_ram_gb)
                      for all running jobs)
                  [steady-state floor: every job capped at its soft limit;
                   represents utilization between bursts]
                  For Batch: soft_limit = preprocess_steady_ram_gb (8% of peak)
                  For K8S:   soft_limit = preprocess_steady_ram_gb (same)

Util₂ (Max Theoretical Utilization)
                = sum(min(job.current_phase_ram_gb, job.soft_limit_ram_gb)
                      for all running jobs)
                  + per_node_max_burst_contribution(node, jobs_on_node)
                  [ceiling: steady-state for all jobs plus the largest subset
                   of job peaks that fits within that node's headroom;
                   computed per node, results summed across pool]

per_node_max_burst_contribution(node, jobs):
                  greedy 0/1 knapsack: sort jobs by peak_ram descending,
                  accumulate peaks until sum > node.spike_headroom_gb_at_launch,
                  return the accumulated sum before overflow
```

**OS overhead treatment:**
- OS overhead is pre-subtracted from Allocated only.
- Utilized and Reserved are measured against net capacity.
- OS overhead never appears as a term in any of the three ratios.
- This treats the OS region as simply unavailable, not as a consumer of capacity.

**Three ratios reported:**

| Symbol | Formula | Answers |
|--------|---------|---------|
| R/A | Reserved / Allocated | What fraction of net capacity did we commit? |
| U₁/A | Util₁ / Allocated | What fraction of net capacity is actively used right now? |
| U₁/R | Util₁ / Reserved | What fraction of committed capacity is actively used right now? |
| U₂/R | Util₂ / Reserved | Best-case burst loading the reservation can support |
| U₃/R | Util₃ / Reserved | Steady-state floor load on the reservation (between bursts) |

**Decomposition signal:**
> `(Util₁ - Util₃) / Allocated` — the burst gap ratio.
> When large and persistent (time-weighted mean > ~10%), workload is a candidate
> for Phase 2 container decomposition.
> When small or brief, decomposition adds complexity without efficiency gain.

**Acceptance Criteria:**
- All five quantities computable from the simulation event log
- All ratios bounded in [0, 1] except Util₁/Reserved under K8S
  (which may exceed 1 during Phase 2 bursts — this is expected and documented)
- Unit test: synthetic 2-job node, known phases, verify all five values exactly

---

## BSIM-62 — Per-tick metric collector for utilization time series

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-61

**Description:**
Extend MetricsCollector to emit a `UTILIZATION_SAMPLE` event every 60 simulated
seconds containing all five quantities.  Requires tracking:
- Per-node: `spike_headroom_gb_at_launch` (set once at node launch)
- Per-job: `current_phase_ram_gb` (updated at every phase transition)
- Per-job: `soft_limit_ram_gb` (set once at job start)
- Per-node roster: which jobs are currently placed

**Implementation note:**
The sampler runs as a SimPy process started at simulation begin, waking every
60 seconds and snapshotting the live NodeModel states.  NodeModel already tracks
`_slots` with per-job phase and RAM draw; extend it to also store
`soft_limit_ram_gb` per slot and `spike_headroom_gb_at_launch` per node.

**Acceptance Criteria:**
- UTILIZATION_SAMPLE events present in event log at 60s intervals
- Each event contains: t, nodes, allocated, reserved, util1, util2, util3
- Aggregator can derive all five ratios from these events without re-simulation

---

## BSIM-63 — Updated utilization charts (three-form, correct definitions)

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-62

**Description:**
Replace the charts produced under BSIM-47 with correctly-defined charts.

**Chart A — Three-percentage time series (two panels):**
- Panel 1: R/A over time — both schedulers
- Panel 2: U₁/A and U₃/A over time — both schedulers, with gap between
  Util₁ and Util₃ shaded to show burst contribution
  (gap = burst gap ratio; shading is the decomposition signal)

**Chart B — Utilization form comparison (bar chart, time-weighted means):**
- X axis: AWS Batch, OKD K8S+
- Three bars per scheduler: U₁/R, U₂/R, U₃/R
- Annotated with R/A and the burst gap ratio
- Clear legend distinguishing instantaneous, max theoretical, min theoretical

**Chart C — Burst gap ratio over time:**
- `(Util₁ - Util₃) / Allocated` for both schedulers
- Reference line at 10% (proposed decomposition-consideration threshold)
- Annotated with "above this line: candidate for Phase 2 decomposition"

**Chart D — Node count over time (context panel):**
- Same as existing node_count_over_time.png, retained for context

**Acceptance Criteria:**
- All charts saved as PNG and SVG in `results/utilization_charts/`
- Chart B correctly shows K8S U₁/R > 100% during bursts without truncation
  (annotated as expected behavior)
- Burst gap ratio mean reported numerically on Chart C
- Charts committed and included in next archive

---

## BSIM-64 — Decomposition signal: workload characterization table

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-63

**Description:**
Produce a per-centroid characterization table showing, for each centroid:
- Mean M (peak RAM)
- Mean S (soft limit / steady-state RAM)
- Mean M/S ratio (how large is the spike relative to steady state)
- Mean advantage_ratio at k=3 (how much K8S benefits this centroid)
- Time fraction in Phase 2 (what fraction of wall-clock time is the burst active)
- Burst gap contribution (this centroid's share of the total burst gap)
- Decomposition recommendation: YES / BORDERLINE / NO

**Decision rule for recommendation:**
- YES:        M/S > 10 AND time_fraction_in_phase2 < 0.15
              (large spike, brief — classic decomposition candidate)
- BORDERLINE: M/S > 5  OR  time_fraction_in_phase2 > 0.15
              (substantial spike or long spike — measure first)
- NO:         M/S ≤ 5  AND time_fraction_in_phase2 ≥ 0.15
              (spike is small relative to steady state, or spike dominates
               the job — decomposition saves little or complicates coordination)

**Acceptance Criteria:**
- Table computed for all centroids in v2 and v3 workloads
- Saved as `results/utilization_charts/decomposition_signal.json` and `.csv`
- Centroid_f and centroid_e expected to show YES
- Centroid_a expected to show NO or BORDERLINE

---

## BSIM-65 — Conclusion slide: three measurements to take before choosing a strategy

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-64

**Description:**
Update the conclusion slide in `docs/presentation.html` to frame the
three required measurements on the real workload before committing to
a scheduling strategy:

**Measurement 1:** `M and S` per job class
  → Feeds the advantage ratio formula
  → Determines whether K8S bin-packing applies
  → Required before choosing k

**Measurement 2:** `(M - M²/C) / S` distribution across job population
  → Shows what fraction of jobs route to Queue 2 at each candidate k
  → Determines whether two-queue routing helps or hurts
  → Required before committing to a node pool architecture

**Measurement 3:** `(Util₁ - Util₃) / Allocated` time-weighted mean
  → The burst gap ratio — decomposition signal
  → If persistently > ~10%, Phase 2 container decomposition is worth evaluating
  → If low, single-container K8S+ with burst pool is the right answer

**Framing:**
These three measurements are not simulation outputs — they are measurements
to be taken on the actual production workload.  The simulation demonstrates
what the measurements mean and what the cost implications of different values
are.  The work required before any prototype is to instrument the existing
Batch workload to collect M, S, and phase timing data per job class.

**Acceptance Criteria:**
- Slide added with the three measurements clearly stated
- Formula `(M - M²/C) / S < k` shown explicitly
- Container decomposition described accurately (PVC coordination, sequential scheduling)
- Measurement instrumentation described as next concrete step
