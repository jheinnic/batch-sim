# CPU Utilization Modeling Decisions

This document records the reasoning behind the CPU scheduling models used
in the simulation for AWS Batch and OKD/K8S+. It is intended to be read
independently of the code when simulation conclusions are questioned.

---

## Why CPU modeling matters for the conclusion

The simulation's central claim is that K8S+ (with hard/soft CPU limits) is
less expensive to operate than AWS Batch for this workload class. For that
conclusion to be defensible, the bias direction of each model must be known
and the combination must not favor the claimed winner:

**Acceptable bias combinations:**

| Winner (K8S+) | Loser (Batch) | Conclusion defensible? |
|---|---|---|
| Understatement | True statement | Yes — winner wins at its floor |
| Understatement | Overstatement | Yes — winner wins despite disadvantage |
| True statement | True statement | Yes — apples to apples |
| True statement | Overstatement | Yes — winner wins despite disadvantage |
| Overstatement | Understatement | **No — conclusion is ambiguous** |
| Overstatement | True statement | **No — conclusion is ambiguous** |

Any analysis that overstates K8S+ efficiency or understates Batch efficiency
makes the conclusion ambiguous and must be corrected before the result is used
to support an architectural decision.

---

## AWS Batch CPU model

### What Batch actually does

AWS Batch on EC2 submits containers via ECS. ECS maps each declared vCPU to
1,024 Linux CPU shares, applied via the cgroup `cpu.shares` mechanism. This
is the CFS (Completely Fair Scheduler) proportional weight model:

- During CPU **contention**, each container receives CPU proportional to its
  share weight relative to the sum of all share weights on the host
- During CPU **idle** periods, any container may use as much CPU as it can
  consume, with no hard ceiling
- There is **no** `cpu.cfs_quota_us` enforcement — Batch does not set hard
  CPU limits on EC2 compute environments

### The simulation model

The simulation uses an iterative proportional allocation solver:

**Pass 0 — initial proportional allocation:**
```
boost_alloc[i] = node_vcpu × (soft_cpu[i] / Σ soft_cpu[j])
```

**Saturation check per job:**
```
if boost_alloc[i] > stage_threads[i]:
    # Thread count saturated — job cannot use more than its thread count
    # regardless of how much CPU the scheduler offers
    boost_alloc[i] = stage_threads[i]
    # Excess is returned to the unsaturated pool for redistribution
```

**Iteration:** Repeat proportional allocation among unsaturated jobs until
either all surplus is distributed or all remaining jobs are saturated.
Maximum passes = number of jobs (O(n)).

**Bug fixed (the implementation did not match this design):** each round
must *add* its share to a job's running total, not overwrite it. The
original code computed `alloc = remaining × (soft_cpu / total_shares)` and
then set `boost_alloc = alloc` directly — so a job that took more than one
round to saturate (or never saturated) lost everything it had accumulated
in earlier rounds, replaced by only its share of that round's much smaller
leftover pool. A node mixing two low-share, high-ceiling jobs with two
higher-share, low-ceiling jobs (32 vCPU total; ceilings 16,16,8,8; shares
1,1,2,2) settled at 2.67 vCPU each for the high-ceiling jobs — 10.67 vCPU
of real, wanted capacity left completely idle — instead of the correct
water-filling result of 8 each, fully using the node. Fixed in
`_distribute_proportional_cfs` (`cpu_boost_integration.py`) by tracking
each round's increment separately and adding it to the job's cumulative
`boost_alloc`.

**Final effective CPU per job:**
```
effective_vcpu[i] = min(stage_threads[i], boost_alloc[i]) × (1 - io_wait[i])
```

**IO-wait treatment:** IO-wait cycles are not explicitly redistributed in the
solver. The proportional formula at steady state already reflects the CFS
behavior: when a thread blocks on IO, CFS immediately offers that quantum to
the next runnable thread in the proportional pool. The iterative solver
captures this implicitly — a job whose threads are IO-blocked has lower
effective consumption, but the kernel does not return those cycles to a pool
that the simulation needs to redistribute explicitly. The proportional
allocation is computed against declared shares, and effective consumption
then reduces by (1 - io_wait) on top of the thread-count-capped allocation.

### Bias direction for Batch

The model is a **true statement** of Batch CPU behavior under the following
conditions:
- All jobs on the node are in the same scheduler cgroup (no nested cgroup
  hierarchy that would change the share denominator)
- The host is not running any non-Batch containers that would affect shares

The model may slightly **overstate** Batch efficiency in edge cases:
- If a job's io_wait fraction causes its threads to release CPU quanta faster
  than CFS can redistribute them (sub-millisecond effects), effective
  utilization is marginally lower than the model predicts
- This overstatement is small and in the direction that favors the loser

**Before the accumulation bug above was fixed, this section's "true
statement" verdict did not hold.** The buggy implementation could
*understate* Batch's effective CPU substantially on any node with more than
one round of saturation — i.e. it modeled Batch jobs running slower, taking
more node-hours, costing more, than the documented design (and real CFS)
would produce. Unlike the K8S+ fix elsewhere in this document, this is
**not** a bias-direction-safe correction: understating the *loser's*
efficiency is not one of the combinations this document's acceptable-bias
table covers as automatically defensible (it only addresses the loser being
modeled as *true or overstated* relative to the winner). If Batch was being
modeled as more expensive than reality while K8S+ was being modeled as only
slightly less efficient than reality, any reported K8S+ cost advantage
could have been partly or wholly an artifact of this bug, not a real
property of either scheduler. **The reference comparison must be re-run
after this fix before any cost-advantage figure derived from it is cited
again** — this fix can change the margin, and is not guaranteed to leave
the winner unchanged.

**Verdict:** The Batch model is a true statement or slight overstatement of
Batch efficiency. This is an acceptable bias direction for the loser.

---

## OKD / K8S+ CPU model

### What K8S actually does

Kubernetes sets two cgroup parameters per container:
- `cpu.shares` from `resources.requests.cpu` (soft weight, identical to Batch)
- `cpu.cfs_quota_us` from `resources.limits.cpu` (hard quota per 100ms period)

When both are set, the hard quota binds: a container cannot consume more than
its limit even if the node has idle CPU. The quota is enforced at 100ms
granularity — a container that exhausts its quota for the current period is
throttled until the next period begins.

### The simulation model (Option 2)

The simulation uses a greedy, non-iterative solver that deliberately withholds
returned cycles from redistribution:

**Surplus computation:**
```
unreserved = node_vcpu - Σ soft_cpu[i]
io_returned_at_soft = Σ (soft_cpu[i] × io_wait[i])
surplus = unreserved + io_returned_at_soft
```

**Greedy allocation (jobs sorted by io_wait ascending — lowest first):**
```
for each job in sorted order:
    headroom = hard_cpu - soft_cpu
    if stage_threads is known (current stage, not the lifetime-peak stage):
        headroom = min(headroom, stage_threads - soft_cpu)
    grant = min(headroom, surplus)
    job.boost_alloc = soft_cpu + grant
    surplus -= grant
    # Cycles returned by this job's io_wait are NOT added back to surplus
    # (Option 2: returned cycles are ineligible for redistribution)
```

**Final effective CPU per job:**
```
effective_vcpu[i] = boost_alloc[i] × (1 - io_wait[i])
```
(`boost_alloc[i]` is now already bounded by `stage_threads[i]` from the
allocation step above, so the `min(stage_threads[i], boost_alloc[i])` clamp
this document previously specified here is no longer a separate step — it is
enforced where the grant is decided, not after the fact.)

### Why Option 2 is correct, not merely conservative

The hard_cpu limit is declared at the maximum any stage of the container will
demand — specifically, the thread count of the most parallel workhorse stage.
The kernel enforces this limit statically for the container's lifetime because
it has no concept of program phases.

A job that returns cycles during a low-demand stage (low thread count or high
io_wait) is not relinquishing its entitlement. Its hard_cpu declaration
accounts for a later stage where it will demand full allocation. If the
returned cycles were redistributed to another job's boost, and both jobs then
advance to their most demanding stages simultaneously, the kernel would face
two containers claiming their full hard_cpu with no headroom to honour either.
This is the CPU starvation condition that the hard limit declaration is
designed to prevent.

Option 2 models this correctly: returned cycles are withheld from
redistribution, exactly as the kernel withholds quota headroom to guarantee
the declared limit is satisfiable on arrival at a demanding stage.

### Fix: a job's *current* stage bounds what it can win, not its lifetime peak

This is a distinct question from the one above, and was a real gap until it
was fixed: how much of the *shared surplus pool* a job can win from other
jobs in the first place, versus what the job is entitled to keep for its own
future use.

The greedy auction originally measured each job's headroom as
`hard_cpu - soft_cpu` — the job's lifetime-peak thread ceiling — with no
regard for whether its *current* stage could use that much. A job in a
single-threaded download/preprocess/upload phase, or any workhorse stage less
parallel than its busiest one, could still win surplus up to its peak
hard_cpu purely by having a low io_wait at that moment, locking capacity into
a grant it had no way to act on while a different, currently-hungrier job
further down the auction order went underserved. The unused portion never
returned to the pool — it simply became `thread_count_waste` charged against
the job that won it, while a job that could have used it got less than it
needed.

This is not the same situation the hard-limit-preservation argument above
protects: that argument is about a job's own entitlement across its *own*
future stages. It does not justify letting a job's *current* incapacity to
use cycles deny those cycles to a *different* job that can use them right
now — real CFS is work-conserving in exactly that sense (an idle share is
reclaimed within the same period by whatever runnable thread wants it). The
fix caps each job's grant at `min(hard_cpu, stage_threads) - soft_cpu`, so a
job can never win more than it can currently use, and the remainder falls
through to the next job in the greedy order instead of being stranded. A
job's cross-stage self-preservation is untouched — it is never granted less
than its current stage can use because of this cap, only prevented from
winning more.

Practical effect: aggregate "effective vCPU" readings for a node can now be
*lower* during windows where a low-io_wait, low-thread-ceiling job was
previously (incorrectly) credited with capacity it could not really consume
— because that reading was never real to begin with. The corrected reading
matches what `engine.py`'s independent `current_vcpu = min(effective_vcpu,
stage_cap)` clamp was already enforcing for actual job progress, so the chart
and real throughput no longer disagree. Total real work done across the node
should be the same or higher than before the fix, not lower, since freed
capacity is now usable by whichever job actually wants it instead of sitting
idle inside an over-generous grant.

### Bias direction for K8S+

The model is an **understatement** of K8S+ CPU efficiency:
- In practice, the Linux CFS work-stealing mechanism may recover some returned
  cycles within a scheduling quantum (1-10ms), making them available to other
  threads before the quota period resets
- The simulation models this at 60-second granularity; sub-second recovery
  is not captured
- Any real-world efficiency gain is therefore at least as large as the
  simulation shows

**Verdict:** The K8S+ model understates K8S+ efficiency. This is an acceptable
bias direction for the winner. The thread-aware auction fix above removes a
previously-undocumented source of *additional*, unintended pessimism that
was not part of this analysis — it strengthens this verdict rather than
changing its direction.

---

## Combined bias assessment

| Scheduler | Model | Bias direction (as of this fix) | Acceptable for conclusion? |
|---|---|---|---|
| AWS Batch | Proportional CFS, iterative (accumulation bug fixed) | True / slight overstatement | ✓, *pending re-run* |
| OKD K8S+ | Option 2 greedy (thread-aware auction fixed) | Understatement | ✓ |

**The combination is now defensible *in direction*, but the *magnitude* is
unverified.** Both fixes above moved their respective model closer to its
documented design. The K8S+ fix only ever strengthens the existing
"understates K8S+" floor argument — safe by construction. The Batch fix is
different: before it, Batch's modeled efficiency could be substantially
*understated* (not just true/overstated), which is outside the bias
combinations this document treats as automatically safe. **Any cost
comparison number on record (including the README's reference run) predates
one or both fixes and must be regenerated before being cited as the current
state of the simulation.**

---

## Waste categories tracked

Three distinct waste categories are tracked per job per scheduling interval:

### 1. io_ineligible_waste (K8S+ only)

Cycles returned by a job's IO wait that cannot be redistributed under Option 2.

```
io_ineligible_waste[i] = boost_alloc[i] × io_wait[i]
```

**Recoverable by:** Container decomposition — separating the IO-bound phase
into its own container with its own (lower) resource declaration allows the
workhorse container to run without the IO-wait ineligibility constraint.

### 2. hard_limit_waste (K8S+ only)

Surplus remaining after every job has been boosted to its hard_cpu ceiling,
its current-stage thread ceiling, or the auction ran dry — capacity nothing
currently running can use.

```
hard_limit_waste = remaining surplus after greedy allocation exhausts all
                   jobs' min(hard_cpu - soft_cpu, stage_threads - soft_cpu)
                   headroom
```

**Recoverable by:** Better workload mix — fewer high-io_wait jobs on the same
node, or larger hard_cpu declarations.

### 3. thread_count_waste (both schedulers)

Cycles allocated to a job that exceed its current stage's thread count. These
cycles are unschedulable regardless of scheduling policy.

```
thread_count_waste[i] = max(0, boost_alloc[i] - stage_threads[i]) × (1 - io_wait[i])
```

**Recoverable by:** Adding more threads to under-threaded stages (code change
to the application). No scheduling policy can recover this waste.

**Note (K8S+, post thread-aware-auction fix):** the K8S+ solver now caps
`boost_alloc[i]` at `stage_threads[i]` during allocation itself (see "Fix: a
job's current stage bounds what it can win" above), so this formula's
`max(0, ...)` term is structurally ~0 for K8S+ going forward — the over-grant
this category used to measure no longer happens; the capacity it used to
count is either used productively by another job or correctly counted as
`hard_limit_waste` instead. Flagged as an open question below: Batch's own
solver (`run_cpu_boost_batch`) also caps `boost_alloc` at `stage_threads`
within its iterative loop by construction, so `thread_count_waste` may be
structurally ~0 there too, not just for K8S+ — this predates today's fix and
was not investigated as part of it.

---

## The two-argument case for K8S+

These waste categories support two distinct arguments:

**Argument 1 — K8S+ wins today:**
Under Option 2 (the conservative/correct model), K8S+ uses surplus CPU cycles
that Batch leaves idle, at lower cost. The advantage is demonstrated at the
efficiency lower bound of K8S+ and at or above the true efficiency of Batch.

**Argument 2 — Container decomposition recovers more:**
io_ineligible_waste is non-zero under K8S+ and is unrecoverable by any
scheduling improvement. It can only be recovered by separating IO-bound phases
into independently-scheduled containers. This waste category does not exist
under Batch (CFS redistributes returned cycles implicitly), which means
container decomposition delivers proportionally larger gains under K8S+ than
under Batch — further strengthening the case for the K8S+decomposition path.

---

## Open questions at time of writing

1. The Batch model assumes all jobs share a single cgroup at the instance
   level. If ECS places jobs in nested cgroup hierarchies, the effective share
   denominator changes. This has not been verified against the ECS
   implementation used in the target environment.

2. The K8S+ model uses a 60-second simulation tick. Sub-second CFS
   work-stealing recovery of returned cycles is not modeled. The magnitude of
   this understatement has not been quantified against real workload data.

3. The io_wait fractions in the centroid configs are declarations, not
   measurements. The simulation's accuracy depends on how well these
   declarations match the actual IO behavior of the real workload. Measurement
   of actual io_wait per stage on the real workload is the recommended next
   step before using the simulation to support architectural decisions.

4. (Found while fixing the thread-aware auction, not investigated further)
   Batch's own solver (`run_cpu_boost_batch`, even after the accumulation-bug
   fix below) still caps `boost_alloc` at `stage_threads` by construction
   within its iterative loop — the same property K8S+'s solver now has
   deliberately. `thread_count_waste` may therefore be structurally ~0 for
   Batch too, not just K8S+, which would mean this waste category — as
   currently computed by both solvers — never actually manifests in either
   scheduler despite being documented and charted as a real, observable
   category for both. Not yet confirmed exhaustively; worth a deliberate
   look before relying on `thread_count_waste` readings for either scheduler.

5. **Both CPU model fixes on record (K8S+ thread-aware auction, Batch
   proportional-accumulation bug) predate any current cost/wait/crash
   figures, including the README's reference run.** Regenerate the
   reference comparison before citing any number from it. Unlike the K8S+
   fix alone, the Batch fix is not guaranteed to be bias-direction-safe (see
   "Bias direction for Batch" above) — re-running is not just a refresh, it
   could change which scheduler the simulation reports as cheaper.
