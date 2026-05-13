# BSIM-E13 — Hard/Soft CPU Limits: K8S CPU Burst Model

Implements the two-argument case for K8S:
  1. K8S with hard/soft CPU limits is cheaper than Batch even for today's
     monolithic container design (uses surplus cycles more efficiently)
  2. Recoverable savings remain that require container decomposition to capture
     (wasted cycles from io_wait ineligibility under Option 2)

---

## BSIM-69 — Schema: workhorse_soft_vcpu and workhorse_hard_vcpu arrays

**Type:** Task | **Priority:** Highest | **Status:** To Do

**Description:**
Add two optional parallel arrays to CentroidConfig, one value per parallel
workhorse stage. The scheduler uses the per-job maxima; the per-stage values
are metadata for the utilization model.

**New fields:**
```yaml
workhorse_soft_vcpu: [2, 4, 2]   # min guaranteed vCPU per parallel stage
workhorse_hard_vcpu: [12, 8, 2]  # thread count = ceiling if surplus exists
```

**Derivation rules:**
```
job.soft_cpu = max(workhorse_soft_vcpu)   # scheduler reservation
job.hard_cpu = max(workhorse_hard_vcpu)   # burst ceiling
```

When absent, existing behaviour is preserved:
```
job.soft_cpu = max(workhorse_thread_counts)  # current declared_vcpu
job.hard_cpu = job.soft_cpu                  # no burst (Batch behaviour)
```

**Validation:**
- Both arrays must have length == len(workhorse_cpu_stages) // 2
- soft_vcpu[i] <= hard_vcpu[i] for all i
- hard_vcpu[i] == workhorse_thread_counts[i] is encouraged but not enforced
  (hard limit is the point where adding vCPU stops helping)

**Theoretical basis (verified):**
12 threads given 4 effective vCPU at io_wait x% == 8 threads given 4 vCPU
at io_wait x%. Thread count only matters when allocated >= threads.
Therefore hard_cpu = max thread count across parallel stages is the correct
ceiling: beyond it, additional vCPU yield zero throughput improvement.

**Acceptance Criteria:**
- Existing configs (no soft/hard arrays) validated unchanged
- New arrays validated: length, soft<=hard per stage
- JobArrivalEvent serialises job.soft_cpu and job.hard_cpu
- inspect_workload.py shows soft/hard per centroid

---

## BSIM-70 — CPU boost solver: Option 2 greedy allocator

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-69

**Description:**
Implement the per-node CPU boost solver that runs at each discrete event
(job arrival, departure, phase transition).

**Algorithm (Option 2 — conservative, non-iterative):**

```
Input:  jobs on node, each with soft_cpu, hard_cpu, io_wait
        node.physical_vcpu

Step 1: Allocate soft_cpu to every job (guaranteed, non-preemptable)
        used = Σ soft_i
        unreserved = node.physical_vcpu - used

Step 2: Compute io_wait surplus
        returned = Σ (soft_i × io_wait_i)
        surplus = unreserved + returned

Step 3: Sort jobs by io_wait ascending (lowest io_wait first)
        These jobs consume the most of whatever they receive.

Step 4: Greedy distribution
        for each job in sorted order:
            headroom = hard_cpu - soft_cpu
            grant = min(headroom, surplus)
            job.boost_allocation = soft_cpu + grant
            # This job's returned cycles go to NO ONE (Option 2)
            # Do NOT add (grant × io_wait) back to surplus
            surplus -= grant
            if surplus <= 0: break

Step 5: Remaining surplus (if any) is WASTED
        wasted_vcpu = surplus
        Record as NodeCPUWaste event
```

**Key properties:**
- Non-iterative: O(n log n) one pass after sort
- Conservative: returned cycles are never redistributed
- Non-blocking: CPU boost never prevents a job from making progress
- Preemptable: when a new job arrives, solver re-runs from Step 1;
  existing boost allocations are reduced without blocking

**io_wait used in solver:**
The solver uses the job's current workhorse stage io_wait if in the
workhorse phase, else 0 (download/upload/preprocess are modelled as
single-threaded or network-bound, not CPU-competing).

**Why Option 2 is correct, not merely conservative:**
The hard_cpu limit is declared at the maximum any stage will demand
(the thread count of the most parallel stage), enforced statically by
the kernel for the container's lifetime. The OS and K8S schedulers have
no concept of phases — this is a simulation construct only.

A job returning cycles in a low-demand phase has not relinquished its
entitlement. Redistributing those returned cycles to another job's boost
would set the kernel up for starvation: when the returning job advances
to its most demanding stage simultaneously with the boosted job, both
claim their full hard limit with no headroom to honour either.

Option 2 prevents this by withholding returned cycles from redistribution
— exactly as cgroup cpu.cfs_quota_us enforcement behaves in practice.
The wasted cycles are the correct accounting of capacity reserved against
future stage demands. K8S+ advantage under Option 2 is a lower bound on
real-world gain, strengthening rather than weakening the pro-K8S case.

**Acceptance Criteria:**
- Unit test: A(soft=4,hard=12,io=0.5), B(soft=4,hard=12,io=0.0)
  → B gets all surplus first (lower io_wait), hits hard limit at 12
  → remaining surplus offered to A; A absorbs up to hard limit
  → wasted = A's returned cycles that cannot be redistributed
- Unit test: new job C(soft=2,hard=2,io=0) arrives mid-boost
  → existing boosts reduced proportionally
  → C's soft guaranteed immediately, no blocking
- Zero wasted CPU when all jobs have io_wait=0

---

## BSIM-71 — Wasted CPU metric: io_wait ineligibility tracking

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-70

**Description:**
Track CPU cycles wasted due to io_wait ineligibility (Option 2 surplus
that cannot be redistributed) as a new simulation metric.

**New event type:** CPU_WASTE
```python
{
    'node_id': str,
    'wasted_vcpu': float,      # vCPU-seconds wasted this interval
    'cause': 'io_wait_ineligible' | 'hard_limit_saturated',
    'job_count': int,
}
```

**New metrics in scorecard:**
- `pool_wasted_vcpu_seconds`: total vCPU-seconds wasted across all nodes
- `pool_wasted_pct_of_allocated`: wasted / total allocated vCPU-seconds
- `per_centroid_wasted_vcpu_seconds`: breakdown by centroid

**New chart (Chart D — CPU waste decomposition):**
Two bars per scheduler:
  - Effective CPU (actually computing)
  - Wasted due to io_wait ineligibility
  - Wasted due to hard limit saturation (job at hard limit, surplus remains)

**Narrative purpose:**
This chart supports both arguments:
  1. K8S uses surplus cycles that Batch leaves idle → K8S wins today
  2. io_wait ineligibility waste is recoverable only by container
     decomposition (separating I/O-bound preprocess into its own container
     with no CPU limit needed, freeing the workhorse container to run
     at full hard_cpu allocation)

**Acceptance Criteria:**
- CPU_WASTE events present in log whenever solver Step 5 wasted > 0
- Scorecard reports wasted_vcpu_seconds and wasted_pct
- Chart D generated alongside existing utilization charts
- Reference workload shows non-zero waste for K8S+ (expected: A's
  returned cycles cannot be redistributed when B is at hard limit)

---

## BSIM-72 — K8S+ scheduler: enforce soft/hard CPU limits in placement

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-70

**Description:**
Update K8SPlusScheduler (and K8SPlusTwoQueueScheduler) to use soft_cpu
for placement decisions and hard_cpu for boost calculations.

**Placement change:**
```python
# Current:
node.allocated_vcpu + job.workhorse_declared_vcpu <= node.physical_vcpu

# New:
node.allocated_vcpu + job.soft_cpu <= node.physical_vcpu
```

Soft_cpu is the scheduler's reservation signal — it determines whether
a job can be placed. Hard_cpu determines the burst ceiling post-placement.

**Batch scheduler:** unchanged. Batch uses declared_vcpu (= soft_cpu)
for both placement and execution — no burst concept. This is intentional:
it makes the comparison honest. Batch reserves conservatively; K8S+
reserves by soft limit and bursts to hard.

**Effective vCPU in utilization metrics:**
Phase-aware actual consumption now uses boost_allocation × (1 - io_wait)
rather than soft_cpu × (1 - io_wait). This raises U1 for K8S+ when
surplus is available and jobs are in compute-heavy stages.

**Acceptance Criteria:**
- K8S+ nodes host more concurrent jobs than Batch on same instance type
  (soft_cpu < declared_vcpu for jobs with io_wait > 0)
- Boost solver runs at each job event on each node
- U1/A and U1/R metrics reflect boosted consumption

---

## BSIM-73 — Updated presentation: two-argument K8S case

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-71, BSIM-72

**Description:**
Update docs/presentation.html with the two-argument structure:

**Argument 1 — K8S wins today:**
"Even with today's monolithic container design, K8S hard/soft CPU limits
allow the scheduler to redistribute cycles that Batch leaves idle.
Under worst-case assumptions (Option 2: no re-redistribution of returned
cycles), K8S+ costs X% less than Batch."

**Argument 2 — Container decomposition recovers more:**
"Y% of provisioned CPU is wasted due to io_wait ineligibility — cycles
that a job returned during an I/O-heavy phase but cannot reclaim during
a subsequent compute-heavy phase. This waste is unrecoverable without
separating the I/O-bound phase into its own container."

**Chart D** (CPU waste decomposition) is the visual anchor for
Argument 2. The burst gap ratio (Util1 - Util3) / Allocated is the
quantitative anchor for Argument 1.

**Acceptance Criteria:**
- Two-argument structure explicit in conclusion slide
- Chart D integrated
- Wasted CPU percentage stated numerically
- Container decomposition recommendation tied to measured waste %
