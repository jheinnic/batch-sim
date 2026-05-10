# BSIM-E5 — AWS Batch Scheduler

---

## BSIM-20 — Batch scheduler interface and saturation model

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-15, BSIM-17

**Description:**
Implement the AWS Batch scheduling strategy. A job's RAM requirement is its Phase-2
peak; its CPU requirement is its Phase-3 maximum declared thread count. A node is
saturated when adding another job would exceed either limit.

**Acceptance Criteria:**
- `BatchScheduler` implements `on_job_arrival`, `on_job_complete`, `guarantee_capacity`
- Saturation: `allocated_ram + job.peak_ram > physical_ram` OR `allocated_vcpu + job.declared_vcpu > physical_vcpu`
- allocated_ram/vcpu = sum of all placed jobs' peak declarations, regardless of current phase
- Scheduler selects most-loaded node that can still fit the job (best-fit decreasing)

---

## BSIM-21 — Batch normal-mode packing logic

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-20

**Description:**
Implement the full normal-mode decision loop: scan existing nodes for fit, hold in
queue if none fit and panic threshold not crossed, launch new instance if needed.

**Acceptance Criteria:**
- Decision fires on: new job arrival, job completion (slot freed), new node becoming available
- Instance type selected via `InstanceRegistry.cheapest_fitting(job.peak_ram, job.declared_vcpu)`
- New instance launch is a SimPy process (warmup delay observed)
- Queue re-evaluated after each node state change

---

## BSIM-22 — Batch panic-mode handler

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-21, BSIM-15

**Description:**
When called for an URGENT job, guarantee_capacity must immediately commit to launching
a new instance sized for that job if no existing node can accommodate it, and reserve
the slot before the instance is warm.

**Acceptance Criteria:**
- Checks all nodes including LAUNCHING ones for eventual fit
- If none will fit, launches new instance immediately
- Reserved slot held exclusively for the URGENT job until placed
- Non-urgent jobs cannot claim a reserved slot

---

## BSIM-23 — Batch scale-down logic

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-22, BSIM-14

**Description:**
When a node becomes idle (last job completes), start the idle timer. After
idle_timeout_seconds with no new job placed, terminate the node and stop cost accrual.

**Acceptance Criteria:**
- Idle timer starts on last job completion
- Node terminated if still idle after idle_timeout_seconds
- NodeTerminationEvent emitted with idle duration
- Cost accrual stops at termination
- finalize() terminates any nodes still running at end of simulation

---

## BSIM-24 — Batch scheduler integration test

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-23

**Description:**
Run BatchScheduler against the shared synthetic event list fixture and verify all
metrics are populated correctly.

**Acceptance Criteria:**
- All jobs in fixture complete or are recorded as terminal failures
- No unhandled exceptions
- Scorecard produced with non-null values for all defined metrics
- Cost total > 0; per-centroid stats populated
