# BSIM-E7 — Metrics & Reporting

---

## BSIM-31 — Event collector and raw metrics store

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-16

**Description:**
Implement the central event collector that receives all typed events emitted during
a simulation run and stores them for post-run aggregation.

**Event types:**
JobArrival, JobQueued, JobStart, PhaseTransition, JobComplete, JobCrash,
JobTerminalFailure, PanicTrigger, BurstCollision, NodeLaunching, NodeReady,
NodeIdle, NodeTerminated

**Acceptance Criteria:**
- `MetricsCollector` receives events via `record(event)` during simulation
- Convenience factory methods for each event type (readable call sites)
- `events_of_type(EventType)` returns filtered list
- Raw event log serializable to JSON via `save(path)`

---

## BSIM-32 — Per-job statistics aggregator

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-31

**Description:**
Derive per-job metrics from the raw event log and aggregate by centroid and globally.

**Per-job metrics:** queue wait time, total execution time, retry count,
SLA breach flag, crash flag, terminal failure flag.

**Aggregate statistics (max, min, mean, stddev) computed:**
- Per centroid
- Pool-level (all jobs)

**Acceptance Criteria:**
- `compute_job_stats(collector, sla_target_seconds)` returns `JobStatsReport`
- All four statistics present for every per-job metric
- Per-centroid dict and pool-level summary both populated

---

## BSIM-33 — Pool-level metrics aggregator

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-31, BSIM-19

**Description:**
Derive pool-level metrics that cannot be attributed to individual centroids.

**Pool metrics:** total cost, cost-over-time series, instance count over time,
node idle time (decomposed: pre-first-job / between-jobs / post-last-job),
overload event count, panic trigger count, burst collision count, terminal failure count.

**Acceptance Criteria:**
- `PoolCostSummary.from_accruers()` produces full cost summary with time series
- `compute_idle_decomposition(collector)` returns IdleTimeDecomposition
- Cost-over-time and node-count-over-time sampled every 60 simulated seconds

---

## BSIM-34 — Scorecard renderer (JSON)

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-32, BSIM-33

**Description:**
Combine job and pool stats into a `Scorecard` dataclass and serialize to JSON.

**Acceptance Criteria:**
- `Scorecard.save(path)` writes structured JSON with all metrics
- JSON includes: scheduler_type, panic_threshold_s, event_list_path,
  job_stats (per_centroid + pool), cost_summary, idle_decomposition, k8s_capacity_report
- `build_scorecard(...)` factory function assembles from collector and accruers

---

## BSIM-35 — Comparative scorecard (Batch vs K8S side-by-side)

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-34

**Description:**
Given two scorecard JSON files (one Batch, one K8S), produce a comparison dict
with delta and ratio columns for key metrics.

**Acceptance Criteria:**
- `compare_scorecards(batch_path, k8s_path)` returns comparison dict
- Delta = K8S value − Batch value; ratio = K8S / Batch
- `python -m batch_sim compare --batch <path> --k8s <path>` renders rich table
  with green highlights where K8S is cheaper/faster

---

## BSIM-36 — Visualization suite

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-35

**Description:**
Produce the key charts used in the Pareto frontier analysis and the presentation.

**Charts:**
1. Cost vs mean wait time scatter (Pareto frontier) — both schedulers
2. Instance count over time — both schedulers overlaid
3. Node idle time decomposition — stacked bar, both schedulers
4. Cost-over-time curve — both schedulers overlaid
5. Per-centroid wait time bar chart with error bars
6. K8S retry count distribution

**Acceptance Criteria:**
- All charts saved as PNG and SVG to `results/plots/`
- Consistent color scheme: Batch = #2C7BB6, K8S = #E08E27
- `generate_all_charts(collated, batch_sc, k8s_sc, output_dir)` generates all six
