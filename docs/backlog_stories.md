---
name: backlog-stories
description: "Queued feature stories and bugs not yet in Jira — covers K8S+, charts, job sizing overhaul, and time-based scheduling policies"
metadata: 
  node_type: memory
  type: project
  originSessionId: 5b6d4b24-a291-419d-8abd-35275275469a
---

## Bug: K8SPlusScheduler costs significantly more than K8S / Batch

**Observation:** After re-enabling K8S+ (added enum value to CLI and schemas), cost is
materially higher than K8S or Batch. Node analysis shows the full job sequence is split
across multiple nodes. ~700+ nodes are completing only one job each; 3 nodes show good
packing. Scheduler is genuinely packing some nodes well but spawning too many single-job
nodes.

**Likely cause:** Semaphore or burst pool logic is preventing re-use of nodes that have
capacity. May also be a `_k8s_fits` / `effective_schedulable_gb` issue carried over from
the base K8S scheduler, amplified by K8S+'s more conservative placement.

**Design intent of K8S+:** Models a real DaemonSet component where each job wraps its
memory-bursting PREPROCESS section in a local hard-limit reservation. This prevents
multiple jobs from simultaneously spiking RAM on the same node (the burst collision that
causes K8S crashes). A staggering mediator could also reduce collisions if preprocess
durations are short/predictable, but cannot compensate for node startup latency at the
end of a quiet period. K8S+ DaemonSet reservation is the better general solution.

**How to apply:** When investigating K8S+ cost anomaly, start with `burst_pool.py` and
`k8s_plus_scheduler.py::_place_job` — look at why nodes are being abandoned after one job.

---

## Bug: generate_node_timelines.py re-run path returns 0 jobs on 0 nodes

**Observation:** With `--event-log` (saved events file), charts render correctly.
Without it (re-run via `run_one()`), the script reports "0 jobs on 0 nodes" even though
`run_one()` returns a non-zero scorecard.

**Likely cause:** `run_and_extract()` calls `run_one(..., return_metrics=True)` and then
builds the timeline from `metrics.log`. The node timeline builder requires both
`NODE_LAUNCHING` and `NODE_TERMINATED` events (`all_node_ids = set(node_launch_time.keys())
& set(node_term_time.keys())`). Nodes that haven't terminated at end-of-sim are excluded.
With a short teeny workload + cool_off, nodes may still be alive (in idle timeout) when
`env.run()` returns if the until= cutoff fires before idle timers fire.

**How to apply:** Check whether `env.run(until=...)` cuts off before idle timers, or
whether `run_one()` in re-run mode fails to call `scheduler.finalize()`.

---

## Chart improvement: use absolute units for CPU and RAM usage panels

**Current:** CPU panel shows "% of peak observed CPU used"; RAM panel shows "% allocated".
**Requested:**
- RAM panel y-axis: GB (not %). For per-node charts use node's own RAM as ceiling.
  For overview chart use total pool RAM as ceiling.
- CPU panel y-axis: absolute vCPU count. Upper bound = instance's own vCPU count
  (per-node) or sum of instance vCPUs across active nodes (overview).
- Remove the "divide by peak" normalization entirely.

---

## Chart improvement: break CPU usage into waste categories

**Requested:** CPU usage panel (bottom of Gantt charts) should show stacked layers:
1. Effective vCPU (green) — cycles doing useful work
2. I/O-ineligible waste (yellow) — cycles blocked on I/O, can't be redistributed
3. Thread-count waste (orange) — allocated above stage thread ceiling
4. Hard-limit waste (red) — cycles withheld by CFS quota (K8S only)

Source data: `CPU_WASTE` events already emitted per node per phase transition.
Aggregate them per-node over time to build the stacked time series.

---

## Chart improvement: CPU panel may only reflect preprocess boost, not steady-state

**Observation:** The usage series builder hard-codes approximate RAM/CPU values per phase
(e.g. workhorse vcpu = 4.0). It doesn't read actual per-stage vcpu from the event log.

**Requested:** Drive CPU/RAM usage from actual phase_transition + CPU_WASTE events rather
than the hard-coded phase approximations in `_build_usage_series()`.

---

## Feature: Replace Pareto job-sizing with discrete size-bin model

**Motivation:** Pareto multiplier makes memory hard to control and doesn't match how
developers submit jobs — they pick the next-highest power-of-2 memory limit, not a
continuous multiplier.

**Design:**
- Replace per-centroid `pareto_multiplier_min/max` with `centroid_bin_weights: list[float]`
  (unnormalized; runtime normalizes to PDF then CDF).
- Single authoritative count: `len(centroid_bin_weights)` determines number of bins.
  Every per-bin array in the centroid config must have this length.
- For each arriving job: draw U ~ Uniform(0,1), find bin index via CDF lookup.
- Parameters that become bin-arrays (one value per bin):
  - `download_size_gb: list[float]`
  - `upload_size_gb: list[float]`
  - `preprocess_duration_s: list[float]`
  - `workhorse_stage_duration_s: list[float]`  (per stage, or a global scale factor)
  - `preloader_hard_limit_gb: list[float]`    — fixed ceiling per bin
  - `preloader_actual_gb: list[[float, float]]` — [lo, hi] uniform draw per bin
  - `steady_state_hard_limit_gb: list[float]`
  - `steady_state_actual_gb: list[[float, float]]`
- Thread counts remain fixed per centroid (not bin-scaled).
- Memory limits are expressed as declared hard limits (what the developer submits),
  not as Pareto-perturbed actuals.

**Example (4 bins):**
```yaml
centroid_bin_weights: [12, 5, 19, 14]   # normalizes to CDF [0.24, 0.34, 0.72, 1.00]
preloader_hard_limit_gb: [8, 16, 32, 32]
preloader_actual_gb: [[4,7], [9,15], [18,24], [24,30]]
```

**How to apply:** Touches `CentroidConfig` (schemas.py), `build_phase_profile()` /
`PhaseProfile` (job_spec.py), and `generate_arrivals()` / sampler (event_list.py or
sampler.py). Keep backward-compat: if `centroid_bin_weights` absent, fall back to
current Pareto path so existing YAML configs still work.

---

## Feature (6b): Time-varying arrival rates and size-bin weights

**Motivation:** Just as the scheduler's queue structure, spawn rates, and drain policies
change by time-of-day (story 7a/b), so does the workload itself. Peak business hours
produce more arrivals, and the mix of job sizes shifts (e.g., larger batch runs submitted
midday; lighter jobs overnight).

**Design:** Parallel to the scheduler's time-window policy structure, each centroid should
support a list of time-window overrides. Each override covers a `[start_time, end_time)`
range and may specify:
- `burst_rate`: arrival rate (bursts/hour) for this centroid during this window
- `centroid_bin_weights`: size-bin weights (same length as the centroid's base definition)
  replacing the default weights during this window

Windows that don't override a parameter inherit the centroid's base value.
Validation: time windows must be non-overlapping and cover the simulation horizon
(or at minimum not leave gaps — unspecified intervals use the centroid baseline).

**Example:**
```yaml
centroids:
  - id: heavy_batch
    burst_rate: 0.5          # baseline: 0.5 bursts/hour (overnight)
    centroid_bin_weights: [20, 15, 10, 5]   # baseline: skews small overnight
    time_windows:
      - start_time_s: 36000  # 10AM
        end_time_s:   50400  # 2PM
        burst_rate: 3.0      # peak: 6× more arrivals during business hours
        centroid_bin_weights: [5, 10, 20, 30]  # peak: skews large during business hours
```

**Relationship to story 7a/b:** The scheduler policy and the arrival model should use
the same time-window boundary scheme so that a single set of breakpoints governs both
"how the scheduler behaves" and "what the workload looks like" at each time of day.
This allows experiments where both sides change together (realistic) or independently
(sensitivity analysis).

**Boundary crossing algorithm (exact — no extra die roll needed):**
At time `t` in a window ending at `W_end` with rate `λ`:
1. Draw `τ ~ Exp(λ)`
2. If `t + τ < W_end`: place arrival, continue from `t + τ`
3. If `t + τ ≥ W_end`: **discard** — place no arrival, advance to `W_end`, start fresh with new rate

This is the exact piecewise-constant Poisson process (independent increments guarantee correctness).
**Do NOT use fraction-of-interval acceptance (Δt/τ_drawn)** — that consistently overestimates arrival
probability because the exponential's heavy tail means long draws are over-represented.
The exact P(arrival before boundary | last arrival at t) = 1 - e^(-λ·(W_end - t)), e.g. 0.393
for a 1hr remaining window at λ=1/2hr, not 0.500.

**How to apply:** Touches `CentroidConfig` (schemas.py) and `generate_arrivals()` in
`event_list.py` or `sampler.py`. The generator walks forward in sim time; at each step,
look up the window containing the current time, draw from that window's rate, and if the
draw crosses the boundary discard and restart at the boundary with the next window's rate.
Bin weight lookup uses the same window resolution.

---

## Feature: Time-based scheduling policy with per-queue memory bands and drain rules

**Goal:** Express K8S scheduling behavior that changes across the day — different queue
counts, memory bands, spawn rates, and node-drain aggressiveness per time window.
Ultimately compare optimized K8S policy configs against real Batch baseline.

**Policy structure:**
- Multiple policy declarations, each with `[start_time, end_time)` covering midnight–midnight
  with no gaps or overlaps.
- Per time window: one or more queues, each with `(exclusive_min_gb, inclusive_max_gb]`.
  Queues must cover 0→max_workload_memory with no gaps/overlaps.
- Per queue:
  - `spawn_instance_class` and `spawn_rate` (new nodes/min while pods unschedulable)
  - Drain rules: list of `{idle_vcpu: N, duration_s: T}` pairs.
    Higher idle_vcpu → shorter required duration. Once a node's idle vCPU exceeds
    threshold N for duration T continuously, node transitions to DRAINING (no new
    pods accepted; existing pods run to completion; node shuts down when last pod exits).

**Validation rules:**
- Time windows must cover [00:00, 24:00) exactly.
- Memory bands per window must cover [0, max_preload_gb] with no gaps/overlaps.
- Drain rules per queue must be monotone: higher idle_vcpu paired with shorter duration.

**Strategic intent:**
- Fewer, larger queues during off-peak (midnight–6:30AM, 6PM–midnight).
- More queues during peak (10AM–2PM) for finer-grained packing.
- Spawning rates tune responsiveness vs. cost at each time of day.
- Draining rules allow controlled scale-down without killing in-flight work.

**How to apply:** New schema objects (TimeWindowPolicy, QueuePolicy, DrainRule) in
schemas.py. K8SScheduler would need to swap its active policy at sim-time boundaries.
Start with schema + validator, then wire into scheduler.

---

## Strategic context (inform all K8S work)

- Primary value: demonstrate K8S advantage over Batch to support platform migration argument.
- Secondary value: once argument is made, use simulation to optimize K8S config
  (instance types, spawn rates, drain policies) before propagating to production.
- Eventually: replace simulated Batch baseline with real Batch execution logs — so K8S
  simulator will be compared against genuine historical data, not a simulated Batch run.
- Focus modeling accuracy on K8S semantics; Batch simulator only needs to be
  "good enough" for comparison baseline purposes.
- Instance ratio context: AWS general-purpose is 4:1 GB/vCPU; compute-optimized 2:1;
  memory-optimized 8:1. Long-tail compute stage favors 2:1; preload spike briefly favors 8:1.
