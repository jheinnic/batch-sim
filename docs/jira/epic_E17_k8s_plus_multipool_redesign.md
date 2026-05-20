# BSIM-E17 — K8S+ Multi-Pool Redesign

Redesigns K8SPlusScheduler from a single-global-policy model to a per-pool
architecture that matches how a real DaemonSet-based system would be configured.
Each memory-tier pool gets its own instance type, headroom reservation, spawn rate,
and age-cordon policy. The headroom per pool is an admin-configured constant, not
derived from scanning the workload — reflecting how operators actually set resource
limits before a cluster goes live.

Also introduces per-scheduler config files to replace the current shared config +
discriminator-argument pattern, which becomes unworkable once each scheduler type
has meaningfully different configuration structure.

Depends on: BSIM-E6 (K8S scheduler), BSIM-E11 (two-queue), BSIM-E13 (CPU limits),
BSIM-83 (TimeWindowPolicy schema from E16)

---

## BSIM-86 — Bug: K8S+ cost anomaly — single-job nodes

**Type:** Bug | **Priority:** High | **Status:** To Do

**Description:**
After re-enabling K8SPlusScheduler, cost is materially higher than K8S or Batch.
Node analysis shows ~700+ nodes completing only one job each; 3 nodes show good
packing. The scheduler is genuinely packing some nodes well but spawning too many
single-job nodes.

**Likely causes:**
- Semaphore or burst pool logic prevents re-use of nodes that still have capacity
- `_k8s_fits` / `effective_schedulable_gb` issue carried over from the base K8S
  scheduler, amplified by K8S+'s more conservative placement
- Burst pool releasing nodes before they can receive a second job

**Investigation targets:**
- `burst_pool.py`: why are nodes being abandoned after one job?
- `k8s_plus_scheduler.py::_place_job`: what condition causes a node with remaining
  capacity to be skipped for the next job?
- Is the headroom reservation (DaemonSet pre-allocation) consuming so much of the
  node that only one job fits, even on nodes that should fit two?

**Acceptance Criteria:**
- Root cause identified and documented in this ticket
- Fix applied and verified: packing ratio (jobs per node) for K8S+ comparable to K8S
  on the same instance type and workload
- K8S+ cost within expected range of K8S (ideally lower, given better burst control)
- Regression test: 12-job teeny workload places multiple jobs per node

---

## BSIM-87 — Schema: per-scheduler config files

**Type:** Task | **Priority:** High | **Status:** To Do

**Description:**
Replace the current shared `SchedulerConfig` + discriminator argument pattern with
separate Pydantic schema classes and separate YAML config files per scheduler type.

Each scheduler type gets:
- Its own `XxxSchedulerConfig` class in `schemas.py` (or a dedicated `schemas_xxx.py`)
- Its own config file under `configs/` (e.g. `k8s_plus_scheduler.yaml`)
- A loader that validates against the correct schema without needing a discriminator

**Motivation:** K8S, K8S+, and Batch have diverging configuration needs:
- Batch: simple, no per-pool concept
- K8S: time-window policies, per-queue spawn rates (E16)
- K8S+: per-pool headroom, age-cordon, DaemonSet reservation (this epic)

A shared schema with optional fields for each scheduler creates a validation surface
that grows without bound and makes it impossible to enforce required fields per type.

**Migration:**
- Existing configs that use the shared schema continue to work during a transitional
  period via an adapter shim; the shim logs a deprecation warning
- New configs use the per-scheduler format exclusively

**Acceptance Criteria:**
- `BatchSchedulerConfig`, `K8SSchedulerConfig`, `K8SPlusSchedulerConfig` are distinct
  classes with non-overlapping required fields
- CLI accepts `--scheduler-config path/to/k8s_plus_scheduler.yaml` and infers the
  correct schema from the file or an explicit `scheduler_type` field in the YAML
- Validation errors name the scheduler type and the offending field
- Existing jch experiment configs migrated to per-scheduler format
- Old shared-config format raises a deprecation warning, not a hard error

---

## BSIM-88 — Schema: K8S+ multi-pool config

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-87, BSIM-83

**Description:**
Define the `K8SPlusSchedulerConfig` schema. A K8S+ deployment consists of one or more
named pools, each serving a memory-tier band. Each pool has its own instance type,
admin-configured DaemonSet headroom reservation, spawn rate, and age-cordon policy.

```yaml
pools:
  - id: small
    exclusive_min_gb: 0
    inclusive_max_gb: 16
    instance_class: m7i.2xlarge
    daemonset_headroom_gb: 18      # admin-configured, not workload-derived
    spawn_rate_per_min: 2.0
    age_cordon_s: 3600             # cordon node after this age even if not idle

  - id: large
    exclusive_min_gb: 16
    inclusive_max_gb: 64
    instance_class: m7i.8xlarge
    daemonset_headroom_gb: 70
    spawn_rate_per_min: 1.0
    age_cordon_s: 7200
```

**Key design decisions:**
- `daemonset_headroom_gb` is the fixed RAM reservation held for the DaemonSet
  component before any jobs are placed. It is set by the operator, not computed from
  the workload — reflecting real deployment practice.
- Memory bands partition the job population exactly as in E16's `QueuePolicy`.
- Time-window overrides (from E16) may optionally be composed with this structure
  to vary spawn rates by time of day, but are not required for the base design.
- `age_cordon_s`: once a node reaches this age, it is cordoned (no new placements)
  even if it still has free capacity. This prevents a quiet period from leaving
  a long-lived node that then fills up with the next burst's jobs, mixing burst
  cohorts on the same node and inflating preprocess RAM contention.

**Validation:**
- Memory bands must be non-overlapping and collectively cover [0, max_workload_gb]
- `daemonset_headroom_gb` must be < instance RAM (obvious sanity bound)
- `instance_class` must exist in the instance registry

**Acceptance Criteria:**
- Schema defined and validated per the above rules
- Reference YAML for jch workload written and validated
- `inspect_config.py` (or equivalent) shows per-pool summary

---

## BSIM-89 — K8S+ scheduler: multi-pool placement with per-pool DaemonSet headroom

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-88

**Description:**
Rewrite `K8SPlusScheduler._place_job()` to use the multi-pool config. A job with
`preprocess_peak_ram_gb = X` is routed to the pool whose memory band contains X.
Within that pool, placement considers:

```
effective_schedulable_gb = node.physical_ram_gb
                         - pool.daemonset_headroom_gb
                         - node.allocated_ram_gb
```

A node fits the job if `effective_schedulable_gb >= job.preprocess_peak_ram_gb`.

The DaemonSet headroom is a fixed per-pool constant reserved at node launch — it
does not vary per job or shrink as jobs are added. This models the DaemonSet component
holding a static reservation for the duration of the node's life.

**Acceptance Criteria:**
- Jobs routed to the correct pool based on memory band
- Placement check uses per-pool `daemonset_headroom_gb`, not a global constant
- Nodes that cannot fit a job due to headroom are skipped without error
- Packing ratio improves over the single-pool baseline (BSIM-86 regression test)
- CPU boost solver (BSIM-70) continues to run correctly after placement change

---

## BSIM-90 — K8S+ scheduler: age-based node cordoning

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-89

**Description:**
Implement age-based cordoning to prevent long-lived nodes from accumulating jobs from
multiple burst cohorts, which inflates preprocess RAM contention.

At node launch, schedule a SimPy process that fires after `pool.age_cordon_s`. When
it fires, mark the node as CORDONED: new placements are rejected but running jobs
continue to completion. After the last job exits, the node terminates normally.

CORDONED differs from DRAINING (E16 BSIM-85) in trigger mechanism:
- DRAINING: triggered by sustained idle vCPU (load-based)
- CORDONED: triggered by node age (time-based), regardless of load

A heavily loaded node may be cordoned and still running its original jobs; a lightly
loaded node may be cordoned and drain quickly. Both are valid outcomes.

**Acceptance Criteria:**
- Cordoned nodes reject new placement attempts
- Existing jobs on cordoned nodes run to completion unaffected
- `NODE_CORDONED` event emitted when cordon fires
- Node terminates with `NODE_TERMINATED` when last job exits (no early shutdown)
- Test: node age_cordon_s=300, jobs arrive at t=0 and t=600 on same node class —
  verify t=600 job lands on a new node, not the cordoned one
- Cordon interacts correctly with DRAINING: a node can be both cordoned and draining
  (whichever state was entered first governs new placement rejection; both conditions
  must clear before the node can terminate)
