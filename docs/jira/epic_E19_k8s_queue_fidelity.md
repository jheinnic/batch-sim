# BSIM-E19 — K8S Queue Fidelity: Named Queues, Configuration-Driven Capacity, Centroid Routing

Corrects a structural mismatch between how the simulator models K8S queue capacity and how
a real K8S deployment actually works.  The current implementation derives spike headroom
from job-spec data (`centroid_peak_rams`), conflating the measurement layer with the
scheduling layer.  A real cluster autoscaler never inspects individual job RAM profiles to
determine node capacity — it reads the node's advertised `allocatable.memory`, which is
fixed at node-pool configuration time.

The fix is to make the queue the authoritative source of the spike reservation.  Each
named queue declares `spike_max_gb` — the non-schedulable region the preprocess semaphore
protects.  The scheduler's bin-packing arithmetic follows directly:

```
effective_schedulable_gb = instance.ram_gb − os_overhead_gb − queue.spike_max_gb
```

This is a hardware constant set when the MachineConfig (node pool) is created.  It does
not change based on which jobs happen to be queued or running.

A secondary fix removes the implicit memory-range routing that required queues to partition
the full memory span without gaps or overlaps.  Jobs now explicitly name their target queue
via a `queue_name` field on the centroid.  This allows queues to overlap in memory range,
differ on non-memory axes (security context, storage class, CPU ratio), and be consolidated
or split across time windows without constraint.

The centroid `queue_name` binding is mandatory (default), with optional per-time-window
overrides.  This enables the "consolidate at night, split at peak" pattern: small-centroid
jobs redirect to a shared queue during low-activity windows, then target their own dedicated
queue as activity rises and finer-grained packing becomes worthwhile.  A queue with no
centroids feeding it in a given window drains naturally; a queue receiving multiple
centroids' traffic provisions nodes normally without any special configuration.

Depends on: BSIM-E16 (time-based scheduling policy schema), BSIM-E17 (K8S+ multipool)

---

## BSIM-100 — Schema: global queue registry and centroid queue_name

**Type:** Task | **Priority:** High | **Status:** To Do

**Description:**
Introduce a global queue registry that separates static hardware configuration (what a
queue's node pool looks like) from per-window behavioural configuration (how aggressively
it provisions and drains).  Add `queue_name` to `CentroidConfig` as the explicit queue
binding.

**New `QueueDefinition` model (global, static):**
```yaml
queues:
  - name: small
    spike_max_gb: 64.0          # non-schedulable semaphore region on every node in this pool
    spawn_instance_class: r7i.4xlarge
  - name: large
    spike_max_gb: 128.0
    spawn_instance_class: r7i.8xlarge
```
`spike_max_gb` and `spawn_instance_class` are hardware constants — they describe the node
pool and do not vary across time windows.

**Updated `QueueWindowConfig` (per-window, behavioural):**
```yaml
time_window_policy:
  windows:
    - start_time_s: 0
      end_time_s: 23400
      queues:
        - name: small           # references global QueueDefinition by name
          spawn_rate: 0.2       # nodes/min while pods pending
          max_nodes: 20         # optional ceiling for this window
          drain_rules:
            - idle_vcpu: 8
              duration_s: 300
```
`spawn_rate`, `max_nodes`, and `drain_rules` may vary per window.  `spike_max_gb` and
`spawn_instance_class` are resolved from the global definition; re-declaring them in a
window entry is an error.

**Centroid `queue_name` (mandatory default):**
```yaml
centroids:
  - id: small_jobs
    queue_name: small           # mandatory; resolved at arrival time
    burst_rate: 2.0
    ...
    time_windows:
      - start_time_s: 0
        end_time_s: 23400
        queue_name: small       # optional override; may redirect to any declared queue
        burst_rate: 0.8
```
`queue_name` on the centroid root is mandatory.  `queue_name` inside a `time_windows`
entry is optional; when present it overrides the default for that interval.

**Schema removals:**
- `QueuePolicy.exclusive_min_gb` — dropped; queues no longer partition the memory span
- `QueuePolicy.inclusive_max_gb` — dropped; replaced by `QueueDefinition.spike_max_gb`
- `QueuePolicy.spawn_instance_class` — moved to `QueueDefinition`
- The old "queues must cover 0 → max_memory with no gaps or overlaps" validation rule

**Acceptance Criteria:**
- `QueueDefinition` Pydantic model validates `name`, `spike_max_gb`, `spawn_instance_class`
- Global `queues: list[QueueDefinition]` field on `SchedulerConfig`; required when
  `time_window_policy` is set, optional otherwise (single implicit queue for no-policy runs)
- `CentroidConfig.queue_name: str` is mandatory; config load fails clearly if absent
- `CentroidConfig.time_windows[*].queue_name: Optional[str]` validated at load time:
  every referenced queue name must exist in the global registry
- Per-window `QueueWindowConfig` validated: `name` must reference a global definition;
  `spike_max_gb` / `spawn_instance_class` in a window entry raises `ValidationError`
- Existing configs without `queues:` or `queue_name:` load with a deprecation warning
  and a single implicit queue named `"default"` with `spike_max_gb` inferred from
  `max(centroid_peak_rams)` — preserving current behaviour for the migration period

---

## BSIM-101 — Centroid routing: per-window queue resolution at arrival time

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-100

**Description:**
Wire the centroid's queue binding into the job arrival path so each job is tagged with
its resolved queue name at the moment it enters the system.

**Queue resolution algorithm (called at `on_job_arrival`):**
1. Look up the centroid for the arriving job
2. Find the first time-window entry whose `[start_time_s, end_time_s)` contains `env.now`
   and has a `queue_name` override
3. If found: use the override; otherwise use the centroid's default `queue_name`
4. Tag the `QueueEntry` with the resolved `queue_name`
5. Admission check (see BSIM-102) — reject immediately if the job cannot fit this queue

**No-policy path:**
When `time_window_policy` is absent, all jobs resolve to the single implicit `"default"`
queue.  Behaviour is identical to the current implementation.

**Observable effects:**
- A centroid targeting `"tiny"` at peak but `"small"` at night causes `"small"` nodes to
  receive arrivals from two centroid families simultaneously during the night window
- A queue receiving no centroid arrivals in a window sees its pending count stay at zero;
  its provisioner idles and existing nodes drain per their drain rules

**Metrics / events:**
- `JOB_QUEUED` event extended with `queue_name` field (already has `centroid_id`)
- Existing `PANIC_TRIGGER` and `SLA_BREACH` events carry `queue_name` for per-queue
  SLA reporting in the scorecard

**Acceptance Criteria:**
- Unit test: centroid with night-window override routes to override queue; outside that
  window routes to default
- Unit test: job arriving at a window boundary (exactly at `start_time_s`) uses the new
  window's queue, not the previous one
- Integration test: two centroids both targeting `"small"` at night produce `JOB_QUEUED`
  events with `queue_name = "small"` regardless of centroid id
- `JOB_QUEUED` events in the saved event log include `queue_name`

---

## BSIM-102 — Capacity model: spike_max_gb from queue config; admission control

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-100

**Description:**
Replace the job-spec-derived spike headroom with the queue's declared `spike_max_gb`.
Add admission control so jobs that cannot fit their queue's spike reservation are
rejected at enqueue rather than spinning in pending until SLA breach.

**`compute_k8s_capacity` signature change:**
```python
# Before
def compute_k8s_capacity(
    instance: InstanceTypeConfig,
    centroid_peak_rams: list[float],
    os_overhead_gb: float = 2.0,
) -> K8SCapacityProfile: ...

# After
def compute_k8s_capacity(
    instance: InstanceTypeConfig,
    spike_max_gb: float,
    os_overhead_gb: float = 2.0,
) -> K8SCapacityProfile: ...
```

`centroid_peak_rams` is removed.  `spike_max_gb` comes directly from `QueueDefinition`.

**Capacity arithmetic (unchanged structure, corrected source):**
```
effective_schedulable_gb = max(instance.ram_gb − os_overhead_gb − spike_max_gb, 0.0)
```

**Capacity cache keyed by `(instance_name, queue_name)`** — same instance type may
have different effective capacity if used in different queues with different
`spike_max_gb`.

**Admission control (at `on_job_arrival`, after queue resolution):**
```
burst_gb = job.profile.preprocess_peak_ram_gb − job.profile.soft_limit_ram_gb
if burst_gb > queue.spike_max_gb:
    emit ADMISSION_REJECTED; trigger panic immediately
```
The job's preprocess burst must fit within the queue's semaphore region.  A job that
names a queue it cannot fit into is an operator configuration error and should surface
as an immediate panic rather than a silent, slowly-mounting SLA breach.

**New event:** `ADMISSION_REJECTED: {job_id, centroid_id, queue_name, burst_gb, spike_max_gb}`

**Chart fix — node timeline spike headroom display:**
The per-node chart currently re-derives `spike_headroom_gb` post-hoc from the jobs that
ran there, producing values that differ across nodes of the same type and do not reflect
what the scheduler used.  After this story, `spike_headroom_gb` shown on the chart is the
queue's declared `spike_max_gb`, read from the node's queue tag.  All nodes in the same
queue show the same value — consistent with the real invariant.

**Acceptance Criteria:**
- `compute_k8s_capacity` no longer accepts or uses `centroid_peak_rams`
- `K8SCapacityProfile.spike_headroom_gb` equals `queue.spike_max_gb` exactly
- Capacity cache key is `(instance_name, queue_name)`; two queues using the same instance
  type but different `spike_max_gb` produce distinct cache entries
- Unit test: queue with `spike_max_gb=64`, r7i.4xlarge (128 GB), `os_overhead=4` →
  `effective_schedulable_gb = 60`
- Unit test: job with `preprocess_peak=80, soft_limit=10` (burst=70) rejected from a
  queue with `spike_max_gb=64`; `ADMISSION_REJECTED` emitted
- Unit test: job with `preprocess_peak=70, soft_limit=10` (burst=60) admitted to same queue
- Node timeline chart shows constant `spike_headroom_gb` for all nodes in the same queue

---

## BSIM-103 — Scheduler partitioning: per-queue provisioning, node tagging, dormant drain

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-101, BSIM-102

**Description:**
Partition the K8S+ scheduler's provisioning and placement logic by queue.  Nodes are
tagged to a queue at launch; jobs are placed only on nodes in their resolved queue.
Queues not active in the current time window stop provisioning and drain naturally.

**Node tagging:**
Each launched node carries a `queue_name` label.  `_launch_node` receives the target
queue as a parameter.  The node's `NodeModel` records `queue_name`; the capacity profile
(`_k8s_capacity`) is looked up by `(instance_name, queue_name)`.

**Per-queue provisioning loop:**
`_try_schedule` iterates over the active queues in the current window.  For each queue:
1. Count pending jobs whose resolved `queue_name` matches this queue
2. Count nodes already running for this queue
3. If pending jobs exist and running nodes are below `max_nodes` cap: evaluate whether to
   spawn a new node using this queue's `spawn_instance_class` and `spawn_rate`

**`_best_fit_node` scoped to queue:**
Only nodes tagged with the job's `queue_name` are considered.  A node in queue `"small"`
never receives a job tagged `"large"`, even if it has free schedulable capacity.

**Dormant queue behaviour:**
A queue not listed in the current time window's `queues:` block is dormant:
- No new nodes are spawned for it
- Existing nodes continue running (jobs in flight complete normally)
- Existing nodes apply their last-active drain rules (the rules from the most recent
  window in which this queue was active); if no prior rules exist, drain rules default
  to a safe conservative policy (e.g., 5-minute idle timeout)
- New arrivals for a dormant queue accumulate in pending; the provisioner does not act
  on them until the queue becomes active again in a later window

**Window transition:**
At each window boundary (`env.now >= window.start_time_s`):
1. Activate newly-listed queues: begin evaluating their pending counts and spawning nodes
2. Deactivate queues not in the new window: freeze their spawn loop; apply drain rules
3. Per-queue drain-rule updates take effect immediately for nodes already running

**Acceptance Criteria:**
- `NodeModel` carries `queue_name`; visible in `NODE_LAUNCHING` event data
- `_best_fit_node` never places a job on a node from a different queue
- Integration test: two queues, two centroid families; confirm no cross-queue placements
  in the event log
- Integration test: queue goes dormant at window boundary — no new nodes spawned after
  the transition; pending jobs for that queue accumulate; jobs resume placement when
  queue becomes active in a later window
- `capacity_report()` output is keyed by queue name in addition to instance type
- Existing single-queue (no-policy) configs produce identical behaviour to the current
  implementation (regression guard)
