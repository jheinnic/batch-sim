# BSIM-118 — What a Comparison Run Must Produce

This is the BSIM-118 spike's design note: it defines what `reproduce_all.sh`'s
successor (BSIM-119's `ExperimentManifest`, BSIM-120's orchestrator) must produce, and
why, before either gets built. Per the epic, this needs review/confirmation before
BSIM-119 starts.

**Revised after the panic-escalation mechanism was removed** (it had no real-world
analog in AWS Batch or Kubernetes/Karpenter — see `docs/jira/epic_E3_core.md`'s BSIM-15
retirement note). §1 and §4 originally treated panic-threshold sweeping as deferred
follow-on scope; it is now simply gone, with nothing to defer.

---

## 1. The comparative claim

**Primary claim this simulation exists to support:** *K8S+ with tiered boost
provisioning costs less than AWS Batch at equivalent SLA on realistic heavy-tailed
workloads.*

K8S (tiers, no Karpenter-style provisioner) is kept as the middle data point it's
always been (RUN-02's baseline pairing) — it isolates how much of the advantage comes
from tiering alone versus from the demand-reactive provisioner (BSIM-86) on top of it.
So the grid needs all **three** production schedulers, not a fixed pair: Batch, K8S,
K8S+. `generate_all_charts`/`build_pareto_frontier`/`detect_meta_effect` are currently
hardcoded to a `("batch", "k8s")` pair — that hardcoding does not survive into the new
design (see §5).

**Variables that must be swept:** scheduler (batch/k8s/k8splus), workload (centroid
mix — currently `jch_centroids_v01`/`v02`). **No longer a sweep axis at all (revised
since this was first drafted):** panic-threshold sweeping. `run_experiment`'s entire
reason to exist was sweeping `panic_threshold_seconds` across scheduler types via
`model_copy` — already impossible once schedulers became a discriminated union (a
`BatchConfig` cannot become a `K8SConfig`), so it was a hard `raise NotImplementedError`
stub even before this revision. Since then, the underlying parameter itself was removed
entirely: `panic_threshold_seconds` modeled a job that's waited too long automatically
escalating to URGENT priority and forcing a dedicated node, and no such mechanism exists
in AWS Batch or Kubernetes/Karpenter (both use static, declared-at-creation priority;
neither escalates by elapsed queue wait) — confirmed by every real run showing
`pool_panic_trigger_count` at zero. There is nothing left to defer. `run_experiment`
remains a stub pending E23 (BSIM-119/120's first cut still ships the workload × scheduler
grid only), but if a sweep axis is wanted later it will need to be something that
actually exists in a real scheduler — not a resurrection of this one.

## 2. Required artifacts per (workload, scheduler) run, and layout

Per BSIM-119's stated convention — confirmed as already the de facto pattern in this
session's own `results/M/` usage (`r2-batch`, `r2-k8s`, `r2-k8splus`, each with a
matching `_events.json` and `_nodes/` dir) — a run is keyed `<workload>-<scheduler>`:

```
results/<set>/
  <workload>-<scheduler>_events.json     # event log (from simulate)
  <workload>-<scheduler>/scorecard.json  # scorecard (from simulate)
  <workload>-<scheduler>-nodes/          # node-timeline diagrams (from render)
    overview.png[, overview_pNN.png]
    <tier>_node_<id>.png  (or node_<id>.png for Batch/untiered)
    summary.json
  comparison.json                        # cross-run aggregate (see below)
```

`comparison.json`: for a 3-scheduler × N-workload grid, a single N-way table beats
pairwise `compare_scorecards` calls (that function is currently 2-scheduler-only too,
named params `batch`/`k8s`). Recommend generalizing it to take a `dict[name,
scorecard_path]` and emit one row per scheduler per workload — cost, mean wait, SLA
breaches, crashes, panics — rather than writing a 3-way special case alongside the
existing pairwise one.

## 3. Canonical workloads + scheduler configs

Use the post-attic-reorg set (BSIM-122/E21), not `reproduce_all.sh`'s current inputs:

- **Workloads:** `configs/jch_centroids_v01.yaml`, `configs/jch_centroids_v02.yaml`.
  Drop `configs/reference_centroids*.yaml` / `workloads/reference_4h_v*.json` — obsolete
  Pareto-path generation, superseded by the discrete bin model these configs use.
- **Scheduler configs:** `configs/jch_batch_scheduler.yaml`, `configs/jch_k8s_scheduler.yaml`,
  `configs/jch_k8splus_scheduler.yaml` — the next-best-equivalent triple derived from the
  same 36-tier set, already passing `validate-tiers` cleanly. Drop
  `configs/scheduler_reference.yaml` — a bare, untiered `K8SConfig`; comparing it
  doesn't speak to the tiering claim at all.

## 4. Provenance

**The incident that motivates this** (already in the epic): a batch of runs labelled
`so*-k8splus` were actually produced by plain K8S — nothing tied the output name to the
scheduler that produced it, and it surfaced only because a human eyeballed crash counts
(K8S crashes-and-retries by design; K8S+ cannot emit a `burst_collision` at all).

**Current state:** `Scorecard` (`metrics/aggregator.py`) already records
`scheduler_type`, `event_list_path` (it also recorded `panic_threshold_s` when this was
first drafted; removed along with the panic mechanism itself). **Missing:**
`scheduler_config_path` (two `K8SPlusConfig`s can share `scheduler_type=k8splus` while
differing in tiers — the type alone doesn't prove which *config* ran) and `seed`.

**Recommendation:** add both fields to `Scorecard`/`build_scorecard`. But recording more
fields doesn't, by itself, fix the incident — the original mismatch was only caught by a
human manually cross-checking output. The orchestrator must do that check automatically:
after each simulate step, assert `loaded_scorecard.scheduler_type ==
manifest.schedulers[<name>].type` for the slot it just populated, and abort loudly on
mismatch. That assertion is what makes a name/run mismatch "ideally impossible" rather
than merely detectable after the fact by a person who happens to look.

## 5. What's dropped from `reproduce_all.sh`, and why

| Run | Disposition | Why |
|---|---|---|
| RUN-01 (Pareto workloads) | **Drop** | Superseded by manifest `workloads:` on canonical configs (§3) |
| RUN-01b (`inspect_workload.py`) | **Keep, but as a standalone manual tool** | Diagnostic, not a comparison artifact; not part of the orchestrated grid |
| RUN-02 (Batch vs K8S compare) | **Drop the script, keep the comparison** | Superseded by the manifest grid + generalized N-way `comparison.json` (§2) |
| RUN-03 (two-queue k-sweep) | Already gone | BSIM-116 |
| RUN-04 (hybrid sweep) | Already gone | BSIM-117 |
| RUN-05 (utilization charts) | **Generalize, not repair-in-place** — see §6 | |
| RUN-06 (node timelines) | **Drop the script, keep the capability** | Superseded by BSIM-120's "Render" phase — already general (any workload × scheduler), already tier-aware, derived from the matching event log by construction |

## 6. Run-05's disposition: generalize via existing per-node data, don't repair the old script in place

Per the explicit BSIM-118 acceptance criterion this spike must satisfy: "drop because
broken" is not on its own an acceptable answer. So — repair, generalize, or drop, with a
reason:

**Not a straight repair.** The break itself is trivial (`cfg_sched.k8s_os_overhead_gb` →
`os_overhead_gb`, stale since BSIM-109), but patching that one line leaves three bigger
problems untouched: the script is hardcoded to exactly Batch vs K8S (no K8S+, doesn't
fit a 3-scheduler grid without rewriting), it reaches into private scheduler internals
(`sched._capacity_cache`, `sched._nodes`) instead of public APIs, and it re-derives
phase windows from the raw event log by hand — logic that already exists, more
generally and tier-aware, in `generate_node_timelines.py`'s `node_timelines`
reconstruction (the same structure behind this session's RAM-usage panel: provisioned /
soft-limit-reserved / in-use / spike-pool-consumed / OS-overhead, per node, per
scheduler, already correct for K8S+).

**Recommendation: generalize.** Don't resurrect the bespoke event-log walk. Compute the
R/A-style ratios (reserved/allocated, instantaneous/allocated, etc.) as a new
aggregation pass over the *existing* `node_timelines` structure, after the render step
already builds it, for whichever schedulers are in the manifest's grid — not a fixed
pair. This is genuine BSIM-119/120-adjacent implementation work, not something to do
inside this spike; flagging it here so it's scoped as a follow-on rather than quietly
dropped or quietly half-fixed.

## 7. Summary: what BSIM-119/120/121 are building toward

- A 3-axis-ready grid (workload × scheduler, threshold deferred) keyed by
  `<workload>-<scheduler>`, with the `_events.json` / `-nodes/` suffixes derived, never
  hand-typed.
- `scheduler_config_path` + `seed` added to `Scorecard`; an automatic provenance
  assertion in the orchestrator, not just more recorded fields.
- A generalized N-way `comparison.json` replacing the 2-scheduler-only
  `compare_scorecards`/`generate_all_charts`.
- Canonical inputs from §3, not `reproduce_all.sh`'s current ones.
- Utilization-ratio reporting reborn as a generalized aggregation over
  `generate_node_timelines.py`'s existing per-node data, scoped as follow-on work, not
  bundled into the first orchestrator cut.
