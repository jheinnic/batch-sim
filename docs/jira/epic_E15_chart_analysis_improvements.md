# BSIM-E15 — Chart & Analysis Improvements

Fixes correctness issues in the node timeline charts and removes percentage-normalized
axes in favour of absolute physical units. Drives CPU and RAM usage series from actual
simulation events rather than hard-coded per-phase approximations. Adds a stacked CPU
waste breakdown panel using the CPU_WASTE events already emitted by BSIM-71.

Depends on: BSIM-E7 (metrics), BSIM-E9 (presentation), BSIM-71 (CPU_WASTE events)

---

## BSIM-79 — Bug: generate_node_timelines.py re-run path returns 0 jobs on 0 nodes

**Type:** Bug | **Priority:** High | **Status:** To Do

**Description:**
When `generate_node_timelines.py` is run with `--event-log` (a saved events file),
charts render correctly. When run without it (re-run via `run_one()`), the script
reports "0 jobs on 0 nodes" even though `run_one()` returns a non-zero scorecard.

**Likely cause:** The node timeline builder requires both `NODE_LAUNCHING` and
`NODE_TERMINATED` events to include a node:
```python
all_node_ids = set(node_launch_time.keys()) & set(node_term_time.keys())
```
Nodes that have not yet terminated are silently excluded. With a short workload +
cooloff, `env.run(until=...)` may fire before idle timers expire, leaving all nodes
alive at sim end with no `NODE_TERMINATED` event.

**Investigation targets:**
- Does `env.run(until=...)` cut off before idle timers fire?
- Does `run_one()` in re-run mode call `scheduler.finalize()`?
- Should the timeline builder include nodes that launched but never terminated
  (treating sim-end as a synthetic termination time)?

**Acceptance Criteria:**
- Re-run path produces the same node count as the `--event-log` path for the same
  workload and seed
- If the fix is a synthetic termination event: clearly documented and tested
- If the fix is ensuring `finalize()` is called: regression test prevents reversion
- `generate_node_timelines.py --help` documents which path is active

---

## BSIM-80 — Chart: absolute units (GB and vCPU) replace percentage normalization

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-79

**Description:**
Replace percentage-normalized axes in the RAM and CPU usage panels with absolute
physical units.

**RAM panel:**
- Y-axis: GB (not % allocated)
- Ceiling: node's own `physical_ram_gb` (per-node charts) or sum of active nodes'
  RAM (overview chart)
- Remove "divide by peak" normalization entirely

**CPU panel:**
- Y-axis: absolute vCPU count (not % of peak observed)
- Ceiling: instance's own vCPU count (per-node) or sum of active node vCPUs (overview)
- Remove peak-normalization

**Acceptance Criteria:**
- Per-node RAM panel y-axis labeled "GB", ceiling = node's `physical_ram_gb`
- Per-node CPU panel y-axis labeled "vCPU", ceiling = node's `physical_vcpu`
- Overview RAM ceiling = Σ active node RAM at each time step
- Overview CPU ceiling = Σ active node vCPU at each time step
- No division by peak or max-observed values anywhere in chart generation

---

## BSIM-81 — Chart: event-driven usage series replaces hard-coded phase approximations

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-80

**Description:**
`_build_usage_series()` currently hard-codes approximate RAM/CPU values per phase
(e.g. `workhorse_vcpu = 4.0`). These do not reflect actual per-stage values from the
event log.

Replace the approximation with a series built directly from `phase_transition` and
`CPU_WASTE` events:
- At each `phase_transition` event, read the actual RAM and vCPU recorded for that
  job/node/phase from the event payload
- Accumulate per-node totals by walking the event log in time order
- CPU series reflects `effective_vcpu` as recorded at each transition (not a
  hard-coded phase constant)

**Acceptance Criteria:**
- CPU series matches sum of `effective_vcpu` slots on the node at each transition point
- RAM series matches sum of `phase_peak_ram_gb` slots on the node at each transition
- No hard-coded phase constants remain in `_build_usage_series()` or callers
- Charts for the reference jch workload show visually correct per-stage CPU steps
  (preprocess spike, then per-stage workhorse steps)

---

## BSIM-82 — Chart: stacked CPU waste breakdown panel

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-81

**Description:**
Add a stacked time-series panel to the per-node Gantt chart showing CPU usage
decomposed into waste categories. Source data is `CPU_WASTE` events already emitted
per node per phase transition by BSIM-71.

**Stacked layers (bottom to top):**
1. Effective vCPU (green) — cycles doing useful work
2. I/O-ineligible waste (yellow) — cycles blocked on I/O, cannot be redistributed
3. Thread-count waste (orange) — allocated above stage thread ceiling
4. Hard-limit waste (red) — cycles withheld by CFS quota (K8S only)

Aggregate `CPU_WASTE` events per node over time to build the stacked series.
Layer boundaries come from the `cause` field on each event.

**Acceptance Criteria:**
- Stacked panel appears below the existing Gantt rows in the per-node chart
- Sum of all layers equals node's `physical_vcpu` at all times (no gaps, no overlap)
- K8S-only hard-limit waste layer is absent (zero) in Batch charts
- Reference jch workload shows non-zero yellow waste for K8S+ (expected: returned
  cycles from io_wait-heavy jobs that cannot be redistributed)
- Panel is skipped gracefully if no CPU_WASTE events are present in the log
