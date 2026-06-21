# BSIM-E20 — Multi-Tier Boost Provisioning

Replaces the E19 named-queue model with a more capable tier-compatibility design that
eliminates the node fragmentation caused by hard single-queue job assignment.

## Problem

E19 introduced named queues with a fixed `spike_max_gb` per queue and required each
centroid to declare exactly one target queue.  This is a correct first model but it
forces the provisioner to treat queues as independent silos.  When two or more queues
share the same `spawn_instance_class`, the provisioner cannot reason across them: it
launches separate nodes for each queue's backlog even when a single differently-configured
node of the same instance type could have served both populations.

Consider three tier profiles all using `r7i.16xlarge`:

```
small_boost:   spike_max_gb=16,  effective_schedulable=238 GB
medium_boost:  spike_max_gb=64,  effective_schedulable=190 GB
large_boost:   spike_max_gb=128, effective_schedulable=126 GB
```

A job whose preprocess burst is 8 GB is physically compatible with all three — any
node's spike zone is large enough.  Under E19 it must be assigned to exactly one tier
by the operator.  A job whose burst is 64 GB is only compatible with `medium_boost`
and `large_boost`.  If the provisioner works per-queue in isolation it will launch one
`small_boost` node for the 8 GB jobs and a separate `medium_boost` node for the
outliers, even though one `medium_boost` node at the same hourly cost could serve both.

## Solution

Replace the single-valued `queue_name` binding on centroids with a multi-valued
`compatible_tiers` declaration.  A job declares the *set* of tier profiles it can
run on.  The provisioner groups pending jobs by `spawn_instance_class`, sees the full
compatibility matrix across all tiers using that hardware, and chooses which tier
configuration to launch to maximise jobs served per node.

A semicolon-delimited string encodes a multi-tier compatibility set within a single
YAML scalar, disambiguating cleanly from the per-bin list form:

```yaml
compatible_tiers: "small_boost;medium_boost"   # same set for all bins
compatible_tiers:                              # per-bin
  - "small_boost;medium_boost"                 # bin 0: either tier
  - large_boost                                # bin 1: large only
```

Node tagging is unchanged: each node belongs to exactly one tier at launch.  Job
placement uses set membership rather than equality.  The provisioner's new decision
is *which* tier configuration to launch for a given batch of pending jobs, not
*whether* to launch for each queue independently.

`QueueDefinition` is renamed `TierProfile`.  The per-window `QueuePolicy` entries
that reference it by name are updated accordingly.  Existing configs using `queue_name`
load with a deprecation warning and are treated as `compatible_tiers` with a
single-element set.

Depends on: BSIM-E19 (named-queue scaffold, node tagging, tuple capacity cache)

---

## BSIM-104 — Schema: TierProfile and compatible_tiers

**Type:** Task | **Priority:** Highest | **Status:** Done

**Description:**
Introduce `TierProfile` as the replacement for `QueueDefinition` and add
`compatible_tiers: str | list[str] | None` to `CentroidConfig` and
`TimeWindowOverride`.

**`TierProfile` (replaces `QueueDefinition`):**
```yaml
tiers:
  - name: small_boost
    spike_max_gb: 16.0
    spawn_instance_class: r7i.16xlarge
  - name: medium_boost
    spike_max_gb: 64.0
    spawn_instance_class: r7i.16xlarge
  - name: large_boost
    spike_max_gb: 128.0
    spawn_instance_class: r7i.16xlarge
```
`name`, `spike_max_gb`, and `spawn_instance_class` are hardware constants that do not
vary across time windows.  Multiple `TierProfile` entries may share a
`spawn_instance_class`; this is the primary use case for the joint provisioner.

**`SchedulerConfig` field change:**
`queues: list[QueueDefinition]` → `tiers: list[TierProfile]`.  `queues` is retained
as a deprecated alias that loads with a warning and maps each entry to a `TierProfile`
with `compatible_tiers` semantics (single-element set per centroid).

**`compatible_tiers` on `CentroidConfig` (replaces `queue_name`):**
```yaml
# Same compatibility set for all bins — semicolons separate tier names within the string
compatible_tiers: "small_boost;medium_boost"

# Per-bin — list position selects bin; each element is a (possibly semicolon-delimited) set
compatible_tiers:
  - "small_boost;medium_boost"   # bin 0
  - large_boost                  # bin 1
```
Type: `str | list[str] | None`.  Same shape as the existing `queue_name` field.

**`compatible_tiers` on `TimeWindowOverride`:**
Same type and semantics.  Overrides the centroid default for jobs arriving during this
window.  A string overrides all bins; a list overrides per-bin.  List length must equal
`len(centroid_bin_weights)`.

**`parse_tier_set(s: str) -> list[str]`** module-level helper in `schemas.py`:
```python
def parse_tier_set(s: str) -> list[str]:
    return [t.strip() for t in s.split(";") if t.strip()]
```

**Validation additions:**
- No tier `name` may contain `;` (reserved as delimiter)
- `compatible_tiers` list length must equal `centroid_bin_weights` length when both set
- `TimeWindowOverride.compatible_tiers` list length validated same way
- Every tier name referenced in `compatible_tiers` must exist in `SchedulerConfig.tiers`
- `queue_name` on `CentroidConfig` and `TimeWindowOverride` raises a deprecation warning
  at load time; its value is silently promoted to a single-element `compatible_tiers`

**Acceptance Criteria:**
- `TierProfile` Pydantic model validates `name`, `spike_max_gb`, `spawn_instance_class`
- `SchedulerConfig.tiers` accepted; `SchedulerConfig.queues` loads with deprecation warning
- `parse_tier_set("small_boost;medium_boost")` returns `["small_boost", "medium_boost"]`
- `parse_tier_set("large_boost")` returns `["large_boost"]`
- Semicolon in a tier name raises `ValidationError` at config load
- Per-bin `compatible_tiers` list with wrong length raises `ValidationError`
- Referencing an undeclared tier name raises `ValidationError`
- Config with `queue_name` loads without error, produces equivalent `compatible_tiers`

---

## BSIM-105 — Job generation: compatible_tiers resolution and propagation

**Type:** Task | **Priority:** Highest | **Status:** Done
**Depends on:** BSIM-104

**Description:**
Resolve each job's compatible tier set at generation time (bin-selection time) and
carry it forward through `JobSpec`, `JobArrivalEvent`, and the event list so the
scheduler has the full compatibility set at arrival without needing to consult the
centroid config.

**`JobSpec` field change:**
`queue_name: str | None` → `compatible_tiers: list[str]` (resolved list, never None
after generation; empty list treated as "no constraint" / legacy fallback).

**`bin_idx: int | None`** is retained on `JobSpec` unchanged — still needed for
time-window override resolution at arrival time.

**Sampler changes (`_sample_job_bin_mode`):**
```python
ct = centroid.compatible_tiers
if isinstance(ct, list):
    raw = ct[bin_idx]          # per-bin element, may be semicolon-delimited string
else:
    raw = ct                   # same string for all bins
resolved = parse_tier_set(raw) if raw else []
return JobSpec(..., compatible_tiers=resolved, bin_idx=bin_idx)
```
Non-bin (`sample_job` Pareto path): `parse_tier_set(ct)` if `ct` is a string, else `[]`.

**Fallback when `compatible_tiers` absent:**
If `centroid.compatible_tiers` is `None` and the scheduler has `tiers` configured,
derive compatible tiers at arrival time from burst arithmetic:
```
min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
compatible = [t.name for t in cfg.tiers if t.spike_max_gb >= min_spike]
```
This is the "admission controller inference" path — correct but less explicit than
declared compatibility.  Emit a one-time `WARN` log per centroid when this path is
taken.

**`JobArrivalEvent` field change:**
`queue_name: str | None` → `compatible_tiers: list[str]`.  `to_job_spec()` and
`_event_from_job()` updated to carry the list.

**`centroid_tier_config` in event list metadata** (replaces `centroid_queue_config`):
```python
"centroid_tier_config": {
    c.id: {
        "compatible_tiers": (
            parse_tier_set(c.compatible_tiers)
            if isinstance(c.compatible_tiers, str)
            else None   # per-bin resolved at generation; not needed in metadata
        ),
        "window_overrides": [
            {
                "start_time_s": w.start_time_s,
                "end_time_s": w.end_time_s,
                "compatible_tiers": (
                    parse_tier_set(w.compatible_tiers)
                    if isinstance(w.compatible_tiers, str) else None
                ),
                "compatible_tiers_by_bin": (
                    [parse_tier_set(e) for e in w.compatible_tiers]
                    if isinstance(w.compatible_tiers, list) else None
                ),
            }
            for w in (c.time_windows or [])
            if w.compatible_tiers is not None
        ],
    }
    for c in config.centroids
},
```
Old event lists carrying `centroid_queue_config` are handled at load time: if
`centroid_tier_config` is absent but `centroid_queue_config` is present, the scheduler
promotes the single-queue values to single-element `compatible_tiers` lists.

**Acceptance Criteria:**
- `sample_job` on a centroid with `compatible_tiers: "small;medium"` produces
  `JobSpec.compatible_tiers == ["small", "medium"]` for every bin
- `sample_job` on a centroid with per-bin list produces the correct per-bin resolution
- `_event_from_job` round-trips `compatible_tiers` through `JobArrivalEvent` without loss
- Event list saved with new format loads back with identical `compatible_tiers` lists
- Old event list with `centroid_queue_config` loads without error; scheduler promotes
  to single-element `compatible_tiers`
- Centroid with no `compatible_tiers` and no `tiers` on scheduler config → `[]`,
  legacy behaviour unchanged

---

## BSIM-106 — Placement: set-membership node compatibility

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-105

**Description:**
Update `_best_fit_node` and the associated `guarantee_capacity` / `_k8s_fits` paths
to use set membership (`tier_name in job.compatible_tiers`) instead of equality
(`node.queue_name == job.queue_name`).  Nodes continue to be tagged to exactly one
tier at launch; the change is entirely on the job side.

**`_best_fit_node` filter change:**
```python
# Before (E19)
job_q = self._job_queue_name.get(job_id)
... and self._node_queue_name.get(node.node_id) == job_q ...

# After (E20)
job_tiers = job.compatible_tiers   # list[str], carried on QueueEntry
... and self._node_tier_name.get(node.node_id) in job_tiers ...
```
When `job.compatible_tiers` is empty (legacy / no-tiers mode), the filter is skipped
and all READY nodes are candidates — preserving the existing no-policy behaviour.

**`_resolve_compatible_tiers(job)` replaces `_resolve_queue_name(job)`:**
Priority order:
1. Time-window override covering `env.now` — string → `parse_tier_set`; list → index
   by `job.bin_idx` then `parse_tier_set`
2. `job.compatible_tiers` set at generation time
3. Fallback: derive from `min_spike` (see BSIM-105)

**`_node_tier_name` replaces `_node_queue_name`** — same dict shape, same lifecycle
(populated at `_launch_node`, cleaned at termination).

**`guarantee_capacity` update:**
When searching for a reserved node for a panicking job, the compatibility check becomes
`node_tier in job.compatible_tiers` rather than equality.  When launching a new node,
select the compatible tier with the smallest `spike_max_gb` that still accommodates
the job's `min_spike` — least wasteful.

**Capacity cache key unchanged:** `(instance_name, tier_name)` tuple from E19 is
already correct for the multi-tier-per-instance case.

**Acceptance Criteria:**
- Job with `compatible_tiers=["small_boost","medium_boost"]` is placed on a
  `small_boost` node when one is available and has capacity
- Same job is placed on a `medium_boost` node when no `small_boost` node is available
- Job with `compatible_tiers=["large_boost"]` is never placed on a `small_boost` or
  `medium_boost` node even when they have free capacity
- `_resolve_compatible_tiers` returns the window-override set when `env.now` is inside
  an override window; returns the job's generation-time set otherwise
- Legacy (no tiers configured): `compatible_tiers` empty → all READY nodes are
  candidates, placement identical to E19 no-policy behaviour

---

## BSIM-107 — Joint provisioner: cross-tier assignment within instance type

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-106

**Description:**
Replace the per-tier independent provisioning loop with a joint provisioner that groups
pending jobs by `spawn_instance_class` and selects the optimal tier configuration to
launch within each group.

**Grouping:**
```
instance_type → { tier_name → [pending jobs compatible with this tier] }
```
A pending job contributes to every tier group it is compatible with.  A job compatible
with `["small_boost","medium_boost"]` appears in both the `small_boost` bucket and the
`medium_boost` bucket for `r7i.16xlarge`.

**Tier selection per node to launch:**
For each instance-type group with unserved overflow:
1. For each tier T, compute virtual packing capacity: how many overflow jobs fit on one
   new node of tier T, given `effective_schedulable_gb = ram - os - T.spike_max_gb`?
   A job is eligible for tier T only if `T.spike_max_gb >= job.min_spike`.
2. Select the tier T* that maximises eligible jobs packed per node.
3. Ties broken by smallest `spike_max_gb` (prefer less wasteful spike reservation when
   packing score is equal).
4. Launch a node of tier T*; subtract its capacity from the virtual overflow; repeat
   until overflow is empty or `max_nodes` / `spawn_rate` limits are hit.

**Dormant tier handling:**
Tiers not listed in the current time-window's `queues` (now `tiers`) block are dormant.
Dormant tiers are excluded from the tier-selection loop: their pending jobs accumulate
but no new nodes are launched until the tier becomes active again.

**Per-tier `max_nodes` and `spawn_rate_per_min`:**
These remain on the per-window `QueuePolicy` (referenced by tier name).  The joint
provisioner respects them per-tier: a tier that has hit `max_nodes` is excluded from
selection even if it would otherwise win.

**`_provision_to_demand` dispatch:**
```python
def _provision_to_demand(self, env):
    if self._tier_defs:
        self._provision_to_demand_joint(env)    # E20 path
    else:
        self._provision_to_demand_legacy(env)   # unchanged
```

**Acceptance Criteria:**
- Scenario: 10 jobs with `min_spike=8` (compatible with all three tiers) + 2 jobs with
  `min_spike=60` (compatible with `medium_boost` and `large_boost` only).  Provisioner
  selects `medium_boost` nodes (not `small_boost`) so all 12 jobs fit with minimum
  node count.
- Scenario: 50 jobs `min_spike=8`, 0 large-spike outliers.  Provisioner selects
  `small_boost` nodes (largest `effective_schedulable_gb`, same cost).
- Scenario: 2 jobs `min_spike=100` (only `large_boost` compatible).  Provisioner
  selects `large_boost` even though it has worst bin-packing score, because it is the
  only tier that can accommodate the burst.
- A dormant tier's pending jobs do not trigger node launches; they resume when the tier
  becomes active in a later time window.
- Per-tier `max_nodes` cap is respected: a tier at capacity is excluded from selection
  even when it would win the packing score.
- Legacy no-tiers path: `_provision_to_demand_legacy` behaviour unchanged.

---

## BSIM-108 — Admission: burst-compatibility validation for declared tiers

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-105

**Description:**
Validate at arrival time that each tier in a job's declared `compatible_tiers` set can
actually accommodate the job's burst.  Warn when a declared tier is burst-incompatible
(the declaration is redundant or erroneous but the job can still be served by other
tiers in its set).  Hard-reject when no tier in the set can accommodate the burst.

**Burst-compatibility check at `on_job_arrival`:**
```python
min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
viable = [t for t in job.compatible_tiers if self._tier_defs[t].spike_max_gb >= min_spike]
incompatible = [t for t in job.compatible_tiers if t not in viable]

if incompatible:
    # Operator declared a tier that can never host this job — log warning
    emit TIER_COMPATIBILITY_WARN: {job_id, centroid_id, incompatible_tiers, min_spike}

if not viable:
    # No declared tier can host this job — reject immediately
    emit ADMISSION_REJECTED: {job_id, centroid_id, compatible_tiers, min_spike}
    trigger panic; return
```
After validation, replace `job.compatible_tiers` with `viable` so the placement and
provisioner never see incompatible tier names.

**`TIER_COMPATIBILITY_WARN` event** (new, non-fatal):
`{job_id, centroid_id, incompatible_tiers: list[str], min_spike_gb: float}`
Surfaces as a scorecard warning counter; does not affect job routing.

**`ADMISSION_REJECTED` event** (existing from E19):
Extended with `min_spike_gb` in addition to existing fields.

**Acceptance Criteria:**
- Job with `compatible_tiers=["small_boost","medium_boost"]` and `min_spike=60`:
  `small_boost` (spike=16) removed from viable; `TIER_COMPATIBILITY_WARN` emitted;
  job routed to `medium_boost` nodes only
- Job with `compatible_tiers=["small_boost"]` and `min_spike=60`:
  viable list is empty; `ADMISSION_REJECTED` emitted; job does not enter the queue
- Job with `compatible_tiers=["small_boost","medium_boost"]` and `min_spike=8`:
  both tiers viable; no warning; no rejection
- `TIER_COMPATIBILITY_WARN` events appear in the scorecard warning count
- After admission, `job.compatible_tiers` contains only burst-viable tiers

---

## BSIM-113 — Zero-headroom (no-boost) tier

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-104

**Description:**
Some workloads have no perceptible preprocess spike: a job allocates its working set at
init (e.g. 32 GB) and holds it flat for its whole run, so `requests == limit` and there
is no short-lived burst to protect with the node-local semaphore. For these, the ideal
node pool reserves *no* boost headroom — 100% of memory (minus OS overhead) is available
for bin-packing. This was the use case the experimental two-queue scheduler tried to
serve with an invisible advantage-ratio heuristic; the tier model expresses it directly
and explicitly instead.

Relax `TierProfile.spike_max_gb` from `PositiveFloat` to `NonNegativeFloat` so a tier can
declare `spike_max_gb: 0` — a no-boost pool where `effective_schedulable_gb =
instance.ram_gb - os_overhead_gb`. A flat job (`preprocess_peak <= soft_limit`, i.e.
`min_spike <= 0`) is burst-compatible with such a tier, and a job with any positive burst
is correctly excluded (and `ADMISSION_REJECTED` if the no-boost tier is its only option).

Because a flat job can list both the zero-headroom tier *and* headroom-bearing tiers in
its `compatible_tiers`, the joint provisioner (BSIM-107) may opportunistically place it
on a node that has spare bin-packing space rather than always spinning up a no-boost node
— no special casing needed; the existing burst/packing math already handles spike=0.

**Acceptance Criteria:**
- `TierProfile(spike_max_gb=0.0)` validates; negative still rejected
- `compute_k8s_capacity(inst, spike_max_gb=0)` → `effective_schedulable_gb =
  ram_gb - os_overhead_gb`, `spike_headroom_gb = 0`
- A flat job (`preprocess_peak == soft_limit`) is viable for a `spike_max_gb=0` tier
- The joint provisioner launches the no-boost tier for a batch of flat jobs
- A positive-burst job whose only compatible tier is no-boost is `ADMISSION_REJECTED`
- `_sem_permits` on a no-boost node degrades to a mutex (1 permit), not a crash

---

## BSIM-122 — Adopt NodeBurstPool in mainline K8S+ (GB-aware concurrent boost)

**Type:** Task (bug-fix / boost-model completion) | **Priority:** High | **Status:** Done
**Depends on:** BSIM-104

**Description:**
The tier `spike_max_gb` reservation (BSIM-102/104) sets the *size* of a node's boost
zone — a provisioning decision that lets the cluster bin-pack by steady-state RAM instead
of by boost peaks. But it says nothing about *who may boost concurrently within that
zone at run time*. That runtime layer is `NodeBurstPool` (BSIM-55): a GB-aware pool that
admits multiple sub-spike jobs into Phase 2 simultaneously as long as their combined
boost fits the reservation, and serialises only when it would overflow.

`NodeBurstPool` was built but only ever wired into the experimental two-queue scheduler.
The mainline `K8SPlusScheduler` still uses the count-based `NodeSemaphore`, whose permit
count is `floor(spike_headroom_gb / tier_local_mm_gb)`. Because `compute_k8s_capacity`
sets both terms equal to `spike_max_gb`, that ratio is **always 1** — the semaphore
degenerates to a per-node mutex, allowing only one boost at a time regardless of how
small individual bursts are or how large the reservation is. (This predates E20: legacy
mode also collapsed both terms, so the count formula never produced >1.) This story lands
the intended fix in the mainline.

Replace `NodeSemaphore` with `NodeBurstPool` in `K8SPlusScheduler`, with two corrections
to the BSIM-55 origin:
1. **Size the pool to the tier's fixed `spike_max_gb`**, not the dynamic `node_max_peak`
   headroom the original computed. The reservation is the budget; it does not grow with
   whatever job happens to land.
2. **Hard invariant: concurrent boost is bounded by the reservation and never borrows
   free bin-packing space.** Momentarily-idle schedulable RAM is committed to future
   placements; lending it to a boost risks the OOM the reservation exists to prevent.

Worked example (the motivating case): a 32 GB node with a 16 GB reservation hosting a
job that needs +6 GB and one that needs +10 GB lets both bootstrap concurrently (6+10 =
16 ≤ 16). Two +6 GB jobs on a node with an 8 GB reservation must serialise (6+6 > 8) even
though bin-packing space is momentarily free — by design.

**Prior art — do not reinvent.** This was already implemented in BSIM-E17 and then
stranded when later work branched from a pre-E17 base. `origin/main`'s
`batch_sim/scheduler/k8s_plus_scheduler.py` (commit `15e22e0`, also on the
`feature/bsim-e17-k8splus-multipool` branch) wires `NodeBurstPool` into the mainline
runner — `burst_pool.acquire(peak)` / `.release()` gating Phase 2, a `self._burst_pools`
dict, and a comment stating "NodeBurstPool with headroom fixed at `daemonset_headroom_gb`
(not workload-derived)" — i.e. it already used correction #1 (pool sized to the fixed
reservation). Port/adapt that implementation onto the current tier-based scheduler rather
than rebuilding from scratch; the differences are: source the headroom from tier
`spike_max_gb` instead of `daemonset_headroom_gb`, and apply the no-borrow invariant (#2).

**Acceptance Criteria:**
- `K8SPlusScheduler` Phase-2 admission uses `NodeBurstPool` sized to the node's tier
  `spike_max_gb` (legacy/no-tier mode: sized to the derived spike headroom)
- Two jobs whose combined burst ≤ reservation boost concurrently; a pair whose combined
  burst > reservation serialises
- A boost request never consumes schedulable (bin-packing) RAM, even when idle
- No-boost tier (`spike_max_gb=0`, BSIM-113) → pool admits at most a mutex's worth (or
  rejects positive-burst jobs upstream via admission control)
- `SEMAPHORE_WAIT` metric still emitted for serialised bursts
- Regression: a tier whose reservation comfortably exceeds combined bursts shows no
  serialisation where the old count-based semaphore would have forced a mutex
