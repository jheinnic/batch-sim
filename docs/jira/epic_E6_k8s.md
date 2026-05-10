# BSIM-E6 — K8S / OKD Scheduler

---

## BSIM-25 — K8S scheduler interface and soft-limit model

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-15, BSIM-18

**Description:**
Implement the K8S scheduling strategy. Provisions by soft-limit RAM (0.08 × peak)
rather than declared peak. A node is schedulable when soft-allocated RAM is below
the effective schedulable ceiling (physical RAM minus spike headroom minus OS overhead).

**Acceptance Criteria:**
- `K8SScheduler` implements `on_job_arrival`, `on_job_complete`, `guarantee_capacity`
- Schedulability: `soft_allocated + job.soft_limit <= node.k8s_effective_ram`
- soft_allocated = sum of soft limits of all placed jobs (regardless of current phase)
- Hard RAM limit per job = job.peak_ram_gb; enforced at node state level, not scheduler level

---

## BSIM-26 — K8S bin-packing and burst collision tracking

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-25, BSIM-13

**Description:**
At every Phase-2 entry, the node checks whether aggregate instantaneous RAM (sum of
all jobs' current-phase RAM) exceeds physical capacity. If so, the overload detector
selects a victim. Two small jobs spiking simultaneously may not crash if their combined
peak fits within physical RAM.

**Acceptance Criteria:**
- Phase-2 entry by any job triggers immediate overload check on its node
- Overload check uses instantaneous RAM, NOT soft allocations
- Two 32 GB-peak jobs on 128 GB node → no crash (combined 64 GB < 128 GB)
- Two 64 GB-peak jobs on 128 GB node → crash (combined 128 GB ≥ 128 GB)
- BurstCollisionEvent emitted with node_id, colliding job IDs, victim, aggregate RAM

---

## BSIM-27 — X% headroom parameter derivation

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-18

**Description:**
For each node type hosting K8S workloads, compute and log the effective headroom
percentage X that the soft-limit strategy implicitly implements. This is a derived
reporting value exposed via `capacity_report()`.

**Acceptance Criteria:**
- headroom_pct = (spike_headroom / physical_ram) × 100
- Reported per instance type in K8SCapacityProfile
- Exposed via `scheduler.capacity_report()` at end of run
- Verified: 128 GB node, 64 GB peak → X ≈ 46%

---

## BSIM-28 — K8S panic-mode handler

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-25, BSIM-15

**Description:**
Implement `guarantee_capacity` for the K8S scheduler. Mirrors the Batch implementation
but uses soft-limit schedulability as the fit criterion. Instance selected must physically
accommodate the job's peak RAM (for burst), not just its soft limit.

**Acceptance Criteria:**
- Same behavioral contract as BSIM-22 but using K8S schedulability check
- New instance sized for `job.peak_ram + os_overhead` to guarantee burst capacity
- Reserved slot held by soft-limit reservation until URGENT job is placed

---

## BSIM-29 — K8S scale-down logic

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-28, BSIM-14

**Description:**
Mirrors BSIM-23 for the K8S scheduler. Identical idle detection and teardown behavior.

**Acceptance Criteria:**
- Same behavioral contract as BSIM-23
- finalize() terminates any nodes still running at simulation end

---

## BSIM-30 — K8S scheduler integration test

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-29

**Description:**
Run K8SScheduler against the shared synthetic event list fixture. Verify burst collision
events are handled and scorecard is fully populated.

**Acceptance Criteria:**
- All jobs complete or recorded as terminal failures
- capacity_report() non-null and contains headroom_pct per instance type used
- Scorecard produced with non-null values for all defined metrics
