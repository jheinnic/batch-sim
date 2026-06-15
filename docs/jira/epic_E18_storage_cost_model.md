# BSIM-E18 — Storage Provisioning Cost Model

Adds EBS storage costs to the simulation so the Batch vs K8S+ comparison reflects
total cost of ownership, not compute alone.

Both schedulers use a **thin-pool-per-node** architecture: each node launches with
a physical LVM thin pool backed by one or more EBS volumes.  Per-job thin logical
volumes (LVs) are created at job start and deleted at job completion, immediately
returning their blocks to the pool.  Pool *capacity* (physical backing) is
monotonically non-decreasing within a node's lifetime; pool *commitment* (sum of
active thin LV sizes) fluctuates with job lifecycle.

The workspace size for each job is derived from `preprocess_peak_ram_gb` — the same
discrete centroid bins that drive memory-tier routing — requiring no new schema fields.

The key difference between schedulers is **packing density**: K8S+ runs more
concurrent jobs per node, driving higher peak commitment and potentially more pool
expansion events per node.  Fewer total nodes may offset this.  The AUC comparison
in BSIM-94 answers whether K8S+ storage overhead erodes its compute savings.

K8S additionally models a **generational thin-pool** strategy (BSIM-93): instead of
expanding the single pool, a new pool generation is opened when an incoming job would
push the current generation past the expansion threshold.  Completed generations are
fully released, bounding stranded capacity to at most one stale generation at a time.
Whether this mitigation is worth the added scheduling complexity is a finding of the
AUC analysis; a follow-on epic (E19) would simulate the generational strategy only if
E18's numbers show material benefit.

Depends on: BSIM-19 (cost accrual model), BSIM-E15 (chart infrastructure)

---

## BSIM-91 — Schema: storage cost config

**Type:** Task | **Priority:** High | **Status:** To Do

**Description:**
Extend the instance registry and scheduler schemas with the fields needed to model
EBS thin-pool storage costs.

**`InstanceTypeConfig` addition:**
```yaml
max_ebs_volumes: 28   # NVMe attachment ceiling for this instance family
```
This bounds the number of physical volumes that can back the thin pool and is used
by the contention model to detect when expansion is no longer possible.

**`StoragePoolConfig` (shared, used by both schedulers):**
```yaml
storage:
  initial_volume_count: 2        # EBS volumes attached at node launch
  volume_size_gb: 1000           # size of each volume (physical)
  logical_capacity_gb: 65536     # LVM thin pool overcommit ceiling (64 TB)
  expansion_trigger_pct: 0.80    # expand when committed > this × capacity
  ebs_price_per_gb_hour: 0.0001096  # gp3 us-east-1 on-demand
```

**Workspace derivation (no new centroid fields required):**
```
workspace_gb(job) = job.profile.preprocess_peak_ram_gb
```
The same discrete memory-tier bins that drive scheduler placement also determine
the job's peak disk working set.  Workspace is declared at dispatch time (known from
the centroid profile), enabling proactive pool management before blocks are written.

**EBS pricing constant:**
`EBS_GP3_PRICE_PER_GB_HOUR = 0.0001096` lives in `batch_sim/registry/instance_registry.py`
alongside the existing compute cost infrastructure.  Scheduler configs may override
it via the `storage.ebs_price_per_gb_hour` field.

**Acceptance Criteria:**
- `InstanceTypeConfig` has `max_ebs_volumes: int` with a sensible default (28)
- `StoragePoolConfig` Pydantic schema validates all fields listed above
- Both `BatchSchedulerConfig` and `K8SPlusSchedulerConfig` accept an optional
  `storage: StoragePoolConfig` block; omission disables storage cost tracking
  without breaking existing configs
- `EBS_GP3_PRICE_PER_GB_HOUR` constant defined and used consistently
- `workspace_gb(job)` helper in `instance_registry.py` returns `preprocess_peak_ram_gb`
- Unit test: 16 GB centroid → workspace_gb = 16.0

---

## BSIM-92 — Batch single thin-pool accrual

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-91

**Description:**
Model the Batch storage pool lifecycle per node.

**Pool state machine (per node):**
- `pool_capacity_gb`: starts at `initial_volume_count × volume_size_gb`; increases
  monotonically when expansion is triggered (one-way — resize or attach)
- `pool_committed_gb`: sum of `workspace_gb` for all jobs currently active on the
  node; rises when a thin LV is created (JOB_START) and falls when it is deleted
  (JOB_COMPLETE or JOB_CRASH)
- Expansion trigger: when `pool_committed_gb > expansion_trigger_pct × pool_capacity_gb`
  AND `attached_volumes < instance.max_ebs_volumes`, add `volume_size_gb` to capacity
- Hard ceiling: `instance.max_ebs_volumes × volume_size_gb`; if committed would exceed
  this and no expansion is possible, emit `STORAGE_EXHAUSTED`

**Cost accrual:**
```
storage_cost_usd = ∫ pool_capacity_gb(t) dt × ebs_price_per_gb_hour
```
Billed on capacity (not commitment); capacity is locked in at the high-water mark for
the remainder of the node's lifetime even as old jobs complete and commitment falls.

**Events emitted:**
- `STORAGE_POOL_EXPANDED`: `{node_id, old_gb, new_gb, committed_gb, trigger_pct}`
- `STORAGE_EXHAUSTED`: `{node_id, committed_gb, capacity_gb}` (capacity ceiling reached)

**Acceptance Criteria:**
- `pool_committed_gb` rises at JOB_START and falls at JOB_COMPLETE/CRASH (verified
  by unit test with two sequential jobs)
- `pool_capacity_gb` only increases; decreases are a bug
- `STORAGE_POOL_EXPANDED` emitted exactly when committed crosses the trigger threshold
- `STORAGE_EXHAUSTED` emitted when committed would exceed `max_ebs_volumes × volume_size_gb`
- Storage cost reported separately from compute cost in the node accruer
- Integration test: node with 4 large jobs (each 600 GB workspace) on a 2 TB initial
  pool triggers exactly one expansion event and bills the correct expanded capacity

---

## BSIM-93 — K8S generational thin-pool lifecycle

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-91

**Description:**
Model the K8S storage pool using a generational strategy that bounds stranded capacity.

Rather than expanding the single pool (which locks in expanded capacity for the node's
remaining lifetime), K8S opens a **new pool generation** when an incoming job's
workspace would trigger expansion of the current generation.  Each generation is a
separate physical thin pool backed by its own EBS volume(s).

**Generational lifecycle:**
1. New job arrives; if `current_gen.committed + job.workspace_gb > expansion_trigger_pct
   × current_gen.capacity`: close the current generation to new placements, open
   generation N+1 with `initial_volume_count × volume_size_gb` fresh capacity
2. Job is assigned to a generation at JOB_START; its thin LV is tracked on that gen
3. When the last job on generation G completes: **release gen G's volume(s)**; storage
   billing for gen G stops at that moment
4. At any point, 2–3 generations may be active simultaneously (old gen's jobs still
   running, new gen receiving incoming jobs)

**Cost accrual:**
```
storage_cost_usd = Σ_gen (gen.capacity_gb × gen.lifetime_s / 3600 × ebs_price_per_gb_hour)
```
where `gen.lifetime_s = gen.last_job_exit_time − gen.open_time`.

**Events emitted:**
- `STORAGE_GEN_OPENED`: `{node_id, gen_id, capacity_gb, trigger: committed_pct}`
- `STORAGE_GEN_RELEASED`: `{node_id, gen_id, capacity_gb, lifetime_s, jobs_served}`

**K8S vs Batch comparison:**
- Batch: stranded capacity = `pool_capacity − pool_committed` for full node lifetime
- K8S: stranded capacity bounded to at most one in-progress generation's idle tail
- The AUC analysis in BSIM-94 quantifies whether this difference is material

**Acceptance Criteria:**
- New generation opened exactly when commitment would trigger expansion
- Job-to-generation assignment stable (a job stays on its assigned gen until completion)
- `STORAGE_GEN_RELEASED` fires when the last job on that generation exits
- Per-node storage cost = sum of all generation costs for that node
- Integration test: 6 large jobs across 2 generations — verify gen 1 releases before
  node terminates, and gen 1's release time equals last-gen-1-job exit time
- Edge case: single-generation node (no overflow) behaves identically to Batch pool

---

## BSIM-94 — Scorecard: compute + storage AUC split, comparison, timeline annotation

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-92, BSIM-93

**Description:**
Surface storage costs in the scorecard and comparison table, and annotate node
timeline charts with pool capacity step functions.

**Scorecard additions (per scheduler):**
```json
"cost_breakdown": {
  "compute_cost_usd": 67.57,
  "storage_cost_usd":  3.21,
  "total_cost_usd":   70.78,
  "storage_pct":       4.5
},
"storage_metrics": {
  "expansion_events":          12,
  "stranded_storage_gb_hours": 840.0,
  "released_capacity_gb_hours": 0.0,    // K8S only
  "nodes_exhausted":             0
}
```

**Comparison table additions:**
- `compute_cost_delta` and `compute_cost_ratio` (K8S vs Batch)
- `storage_cost_delta` and `storage_cost_ratio`
- `total_cost_delta` and `total_cost_ratio`
- Summary finding: does K8S storage overhead erode the compute saving? (boolean + magnitude)

**Node timeline chart annotation:**
- Per-node chart: add a `pool_capacity(t)` step function as a shaded band below the
  RAM panel — shows physical storage ceiling in GB over the node's lifetime
- Mark `STORAGE_POOL_EXPANDED` events as vertical lines on the storage band
- For K8S: shade each generation in an alternating colour; `STORAGE_GEN_RELEASED`
  marked with a drop to zero
- Overview chart: pool-wide `Σ pool_capacity(t)` step function (storage provisioned
  across all active nodes at each time step)

**Acceptance Criteria:**
- `compute_cost_usd + storage_cost_usd == total_cost_usd` to within floating-point error
- `python -m batch_sim compare` table shows all three cost columns (compute, storage, total)
- Green highlight when K8S total is lower; amber when storage overhead partially offsets
  compute saving; red when storage overhead exceeds compute saving
- Per-node chart renders pool capacity band without overlapping the RAM panel
- Scorecard JSON schema documented; existing scorecards without storage fields
  load without error (storage fields default to None)
