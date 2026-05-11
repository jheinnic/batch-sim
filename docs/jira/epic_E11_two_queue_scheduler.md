# BSIM-E11 — Two-Queue Advantage-Ratio Scheduler, k-Sweep, and Degenerate Workload

---

## BSIM-53 — Advantage ratio formula and queue-assignment module

**Type:** Task | **Priority:** Highest | **Status:** To Do
**Depends on:** BSIM-50 (K8S+ semaphore scheduler)

**Description:**
Implement the queue-assignment decision as a standalone, testable function.
For a given job and candidate instance type, compute the advantage ratio and
classify the job into Queue 1 (K8S bin-packing applies) or Queue 2
(near-capacity; degraded to longer-wait provisioning).

**Formula:**
```
advantage_ratio = (M - M²/C) / S

where:
  M = job.preprocess_peak_ram_gb      (actual peak from Pareto sample)
  S = job.preprocess_steady_ram_gb    (actual steady-state; NOT assumed to be 0.08M)
  C = cheapest_fitting_instance.ram_gb
```

**Queue routing:**
```
advantage_ratio >= k  →  Queue 1 (bin-packing, standard panic threshold)
advantage_ratio <  k  →  Queue 2 (near-capacity, queue2_panic_multiplier × panic threshold)
```

**Implementation:**
- `batch_sim/scheduler/queue_router.py`
- `compute_advantage_ratio(M, S, C) -> float`
- `assign_queue(job, instance_registry, k) -> tuple[QueueClass, InstanceTypeConfig]`
  - Iterates registry sorted by price ascending
  - First instance where M ≤ instance.ram_gb is `cheapest_fitting`
  - Computes advantage_ratio using that instance's C
  - Returns (QueueClass.ONE or QueueClass.TWO, cheapest_fitting)
- `QueueClass(str, Enum)` with values `Q1 = "q1"` and `Q2 = "q2"`

**Critical note:** S is taken from the actual job profile, not derived as 0.08 × M.
The 8% figure is a simulation convenience; the formula must use the real sample value
so the degenerate condition is correctly detected when real jobs diverge from that ratio.

**Acceptance Criteria:**
- `compute_advantage_ratio(M=64, S=5.12, C=128)` returns `(64 - 64²/128) / 5.12 = 6.25`
- `compute_advantage_ratio(M=118, S=9.44, C=128)` returns `(118 - 118²/128) / 9.44 ≈ 0.82` (degenerate)
- `assign_queue` correctly selects cheapest fitting instance in one sorted pass
- Unit tests cover: comfortable job, near-capacity job, job that exceeds all instances (returns None)
- No coupling to any specific scheduler class

---

## BSIM-54 — Two-queue K8S+ scheduler (K8SPlusQueuedScheduler)

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-53, BSIM-50

**Description:**
Extend K8SPlusScheduler to maintain two separate job queues with independent
panic thresholds and node pools. Queue 1 uses the existing semaphore bin-packing
logic. Queue 2 uses a longer panic threshold and is routed to its own node pool
to prevent large-spike jobs from contaminating Queue 1 nodes.

**Configuration additions to SchedulerConfig:**
```python
advantage_k: float = 4.0          # Queue 1/2 split threshold; sweep variable
queue2_panic_multiplier: float = 3.0  # Queue 2 panic = base × multiplier
```

**Behavioral specification:**

Queue 1 nodes:
- Provisioned for the Queue 1 job population's tier-local MM
- Semaphore permits derived from `floor(spike_headroom / job.peak_ram)` using
  actual job M, not tier MM (per the dynamic semaphore fix from Gap 2 discussion)
- Headroom = `C - os_overhead - max(M for jobs actually scheduled to this node)`
  Updated each time a new job is placed, so the headroom shrinks correctly

Queue 2 nodes:
- Each Queue 2 job gets the cheapest physically-fitting instance
- Panic threshold = `cfg.panic_threshold_seconds × cfg.queue2_panic_multiplier`
- No semaphore needed (near-single-tracking means collisions are already unlikely;
  document this explicitly in code comments)
- Tracked separately in cost accounting so Queue 1 and Queue 2 costs are reported
  independently

**Scorecard additions:**
- `q1_job_count`, `q2_job_count`
- `q1_cost_usd`, `q2_cost_usd`
- `q1_mean_wait_s`, `q2_mean_wait_s`
- `q1_crash_count` (should be near zero), `q2_crash_count`
- `advantage_ratio_distribution`: histogram of advantage_ratio values across all jobs
  (shows how the population splits as k varies)

**Acceptance Criteria:**
- Queue 2 jobs never appear on Queue 1 nodes and vice versa
- Changing k from 2 to 4 moves measurably more jobs into Queue 1
- Queue 2 panic threshold is exactly `queue2_panic_multiplier × base`
- Cost breakdown by queue visible in scorecard

---

## BSIM-55 — Dynamic semaphore permits based on actual job M (not tier MM)

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-50

**Description:**
Fix the semaphore permit calculation to use the actual peak RAM of jobs
being placed on a node, not the tier-local MM. This implements the
physically correct model: two jobs may spike simultaneously if and only if
their combined peak RAM fits within the node's spike headroom.

**Current (incorrect):**
```python
permits = floor(spike_headroom / tier_local_MM)  # fixed at node launch
```

**Corrected model:**
The semaphore is not a fixed-permit counter. Instead, before entering Phase 2,
each job requests a "RAM reservation" of M GB from a node-level burst pool.
The burst pool has total capacity = spike_headroom GB.
The request blocks until M GB is available; releases M GB on Phase 2 exit.

**Implementation:**
- Replace `NodeSemaphore` fixed-permit model with `NodeBurstPool`
- `NodeBurstPool(env, headroom_gb)`:
  - `acquire(job_id, peak_ram_gb)` → SimPy generator; blocks until `headroom_gb - in_use >= peak_ram_gb`
  - `release(peak_ram_gb)` → returns that RAM to the pool
- Two small jobs each needing 20 GB can both spike simultaneously on a 128 GB node
  with 59 GB headroom (20 + 20 = 40 ≤ 59) — correct
- One large job needing 55 GB blocks the second until the first releases — correct
- `headroom_gb` is set at node launch and does NOT use tier MM; it uses the
  actual maximum M among jobs placed on this node so far, updated on each placement

**Note on headroom update:**
When a new job is placed on a node, if its M exceeds the current node max_M,
the burst pool's headroom must be recomputed:
  `new_headroom = C - os_overhead - new_max_M`
  If `new_headroom < current_in_use`, existing Phase-2 jobs are not interrupted
  (they already acquired their reservation); new acquisitions simply block until
  headroom is sufficient. This is the correct physical behavior.

**Acceptance Criteria:**
- Two 20 GB-peak jobs spike simultaneously on a node with 59 GB headroom: no blocking
- One 55 GB-peak job blocks a second 55 GB job until first releases: correct serialization
- Zero crashes in K8S+ runs (burst pool prevents all overloads)
- Unit test: node with 128 GB physical, 3 jobs queued with M = 20, 20, 55 GB

---

## BSIM-56 — k-sweep experiment runner

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-54, BSIM-53

**Description:**
Extend the experiment runner to sweep k ∈ {2, 3, 4} across the two-queue
K8S+ scheduler, alongside single-queue Batch and K8S+ as baselines.
Produce a sensitivity curve showing how cost, crash rate, queue split, and
mean wait time change as k varies.

**Sweep design:**
```
schedulers: [Batch, K8S+ (single queue, existing), K8S+ Two-Queue (k=2), k=3, k=4]
event_list: reference v2 workload (301 jobs, spike-heavy mix)
panic_threshold: 300s (fixed; Queue 2 uses 3× = 900s)
```

**Outputs per k value:**
- Total cost (USD)
- Q1 job count, Q2 job count, and % in each queue
- Q1 cost, Q2 cost
- Mean wait time (Q1 and Q2 separately)
- Crash count (should be 0 for K8S+ two-queue)
- Advantage ratio histogram (distribution of actual ratios in the job population)

**Sensitivity curve chart (new Chart 9):**
- X axis: k value (2, 3, 4)
- Y axis left: total cost USD
- Y axis right: % jobs routed to Queue 2
- Series: K8S+ Two-Queue cost, Batch cost (flat reference line)
- Annotation: point where K8S+ Two-Queue crosses Batch cost (if it does)

**Decision criteria for extension to {5, 6, ...}:**
After reviewing the k ∈ {2, 3, 4} curve:
- If cost is still declining steeply at k=4: extend to {5, 6}
- If cost has flattened but not reversed: extend to {6, 8}
- If cost is reversing (Queue 2 overhead exceeding Queue 1 savings): extend to {5, 7, 10}
  to characterize the reversal shape
- If flat throughout: the workload is insensitive to k and that is itself the finding

**Acceptance Criteria:**
- Sweep runs end-to-end without manual intervention
- All five scheduler configurations produce scorecards
- Sensitivity curve chart generated and saved
- Decision criteria evaluated and next k values documented in `results/k_sweep/README.md`

---

## BSIM-57 — Degenerate workload: extreme spikers with and without pool isolation

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-56

**Description:**
Design and run a workload that demonstrates both failure modes and the isolation solution:

**centroid_f — "Extreme Spiker":**
```yaml
id: centroid_f
label: "Extreme Spiker"
description: >
  Jobs whose peak RAM is deliberately sized to land in the degenerate zone
  on mid-tier instances (advantage_ratio < 2 on anything below r7i.8xlarge)
  but recover to Queue 1 territory on r7i.8xlarge (256 GB).
  Demonstrates that right-sizing the node pool recovers the K8S advantage.
arrival_rate_per_hour: 8
pareto_alpha: 1.8       # heavy tail — some jobs exceed even 256 GB
download_gb: 60.0
preprocess_memory_exponent_a: 2.2
preprocess_memory_exponent_b: 1.8
preprocess_duration_seconds: 110
workhorse_cpu_stages: [200, 30, 400, 25]
workhorse_thread_counts: [8, 8]
io_wait_fraction: 0.25
upload_gb: 3.0
```

Nominal peak for centroid_f: `2.2 × 60^1.8 ≈ 2.2 × 1161 ≈ 2554 GB`

Wait — that exceeds all available instances. Let me recalibrate:

Use download_gb: 15.0 → `2.2 × 15^1.8 = 2.2 × 130 ≈ 286 GB`
On r7i.4xlarge (128 GB): M/C = 286/128 = 2.23 → exceeds node, can't fit
On r7i.8xlarge (256 GB): M/C = 286/256 = 1.12 → still exceeds node
On r7i.16xlarge (512 GB): M/C = 286/512 = 0.56 → fits; advantage_ratio = (286 - 286²/512)/S

With S ≈ 0.08 × 286 = 22.9:
advantage_ratio = (286 - 159.7) / 22.9 = 126.3 / 22.9 ≈ 5.5 → Queue 1 at k=4 ✓

So centroid_f (download_gb=15) lands on r7i.16xlarge (512 GB), is Queue 1 at k≤5.
Jobs with Pareto draws that push peak above 470 GB (M/C > 0.92 on 512 GB node)
become degenerate and route to Queue 2, demonstrating the heavy-tail failure mode.

**Two experimental conditions:**
1. Combined pool: centroid_f mixed with other centroids in a single K8S+ queue
   → shows large-spike jobs consuming headroom that blocks smaller jobs
   → shows advantage_ratio collapse for the large-spike subpopulation
2. Isolated pool: centroid_f routed to a dedicated r7i.16xlarge pool
   → shows cost recovery for the overall system
   → shows Queue 2 economics for the Pareto-tail jobs that exceed even 512 GB

**Acceptance Criteria:**
- Combined pool run shows measurable degradation vs. isolated pool run
- Advantage ratio histogram for centroid_f shows bimodal distribution
  (most jobs in Queue 1 territory, tail in Queue 2)
- Cost difference between combined and isolated documented
- centroid_f config committed to `configs/`

---

## BSIM-58 — OKD/K8S provisioner abstraction: NodePool model

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-54

**Description:**
Model the OKD Machine Autoscaler / MachineSet abstraction correctly.
The provisioner receives a binary "pods pending in this pool" signal per pool,
not job-level resource information. It responds by adding one machine of the
pool's fixed instance type.

**Implementation:**
- `NodePool` dataclass: `pool_id`, `instance_type`, `queue_class` (Q1 or Q2),
  `min_nodes`, `max_nodes`
- `MachineAutoscaler` per pool: fires when a job in that queue has been pending
  longer than `scale_up_delay` (separate from panic threshold — this is the
  infrastructure response time, not the scheduler's obligation timer)
- The autoscaler does NOT inspect job resource requests — it only knows a pod
  is pending in its namespace
- Scale-down: node removed when idle for `idle_timeout` (same as current model)

**Gap 3 misconfiguration scenario (BSIM-59):**
Implement a "misconfigured" variant where the pool routing exists but the
node selector / taint is wrong, causing all jobs to land in the same pool
regardless of queue assignment. Show the cost and crash rate degradation.

**Acceptance Criteria:**
- NodePool and MachineAutoscaler classes implemented
- Correct configuration matches expected cost from BSIM-56
- Misconfigured variant documented and run; results compared in scorecard

---

## BSIM-59 — Gap 3: misconfiguration demonstration

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-58

**Description:**
Run the two-queue system with correct routing, then re-run with a misconfiguration
where the taint/node-selector binding is absent (all jobs land in one pool).
Compare cost, crash rate, and wait time to show the operational penalty.

**Specific misconfiguration:** Queue 2 jobs routed to Queue 1 nodes because
the node selector label is missing from the job spec. Large-spike jobs consume
Queue 1 node headroom, reducing Q1 bin-packing efficiency and increasing crashes.

**Acceptance Criteria:**
- Misconfigured run shows higher crash count than correct configuration
- Cost difference documented
- Commentary added to presentation noting that this failure is silent
  (no error thrown; scheduler happily places jobs in the wrong pool)

---

## BSIM-60 — Conclusion slide: degenerate condition and container decomposition

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-57

**Description:**
Add a conclusion slide to `docs/presentation.html` that:

1. States the degenerate condition explicitly in plain language:
   "When a job's peak RAM exceeds roughly 60-80% of the node it runs on
   (depending on your k threshold), K8S bin-packing offers little advantage
   over AWS Batch. The scheduler sophistication cannot overcome the physics."

2. Shows the formula `(M - M²/C) / S < k` as a litmus test operators can
   apply to their own workload measurements of M and S.

3. States that the only architectural escape from this constraint is to
   decompose the peak-RAM phase into a separately-scheduled container:
   - Container A: Download + Preprocess → writes output to a PersistentVolumeClaim
     (or equivalent durable shared storage that tolerates pod relocation)
   - Container B: Workhorse + Upload → reads from the same PVC, scheduled
     independently with lower RAM requirements
   - Container A and B are scheduled consecutively, not concurrently
   - This allows Container A to be provisioned for its peak RAM requirement
     and Container B to be provisioned for its much lower steady-state requirement
   - The K8S Job API with init containers or sequential job steps is the
     natural mechanism; do not overstate the implementation complexity but
     do not understate the coordination burden either

4. Notes that measuring actual M and S on the real workload — not assuming
   S = 0.08M — is a prerequisite for choosing k, and that this measurement
   work is the recommended next step before committing to a provisioning strategy.

**Acceptance Criteria:**
- Slide added to presentation with formula rendered clearly
- Container decomposition described accurately (PVC coordination, sequential scheduling)
- No overpromise on implementation complexity
- Language reviewed against the actual simulation results before finalizing
