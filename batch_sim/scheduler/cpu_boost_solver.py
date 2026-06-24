"""
BSIM-70: CPU boost solver — Option 2 greedy allocator.

Distributes surplus vCPU cycles to jobs on a node beyond their soft_cpu
reservation, up to their hard_cpu ceiling.

Option 2 semantics (correct, not merely conservative):
  - Jobs are sorted by io_wait ascending (lowest io_wait first)
  - Each job absorbs surplus up to its hard_cpu headroom, capped at what
    its CURRENT stage can actually use (stage_threads) — a job cannot win
    more of the shared surplus than it is presently capable of consuming,
    regardless of its lifetime-peak hard_cpu declaration
  - Cycles returned by a job's I/O wait are NOT redistributed
  - Surplus a job's current stage cannot use falls through to the next
    job in the greedy order instead of being locked into an unusable grant
  - Any surplus remaining after every job is satisfied — at its hard_cpu
    ceiling or at its current stage's thread ceiling — is permanently
    wasted for the current scheduling interval

Why Option 2 is physically correct, not merely pessimistic:

  The hard_cpu limit is declared at the maximum any stage of the
  container will demand — specifically, the thread count of the most
  parallel stage. The kernel enforces this limit statically for the
  container's lifetime because the OS has no concept of phases.

  A job that returns cycles in one of its OWN stages is not relinquishing
  its entitlement — it is in a phase where it cannot use what it is
  entitled to, and that entitlement remains declared at the hard limit for
  when a later stage demands it. Option 2 protects this by never handing a
  job's unused headroom to a DIFFERENT job's boost allocation: doing so
  would set the kernel up for starvation if the first job's next stage
  arrives at its maximum threading capacity simultaneously with the
  boosted job's demand, with two jobs claiming their full hard limit and
  no headroom to honour both.

  That is a distinct question from how much of the shared surplus pool a
  job can win in the first place. A job whose current stage is only
  single-threaded cannot productively use eight vCPU of surplus just
  because its hard_cpu declares an eight-thread ceiling for some other,
  more parallel stage — granting it that much anyway protects nothing (it
  has no current use for cycles it would be "returning"), it merely
  strands capacity a different, currently-hungrier job could have used.
  Real CFS is work-conserving in exactly this sense: an idle share is
  reclaimed within the same period by whatever runnable thread can use it.
  Capping each job's grant at its current stage_threads, and letting the
  remainder fall through to the next job in the greedy order, restores
  that property without giving up any of a job's cross-stage
  self-preservation — a job is never penalised for what it might need
  later, only prevented from winning more than it can use right now.

  The wasted cycles after this capping are therefore not a modelling
  pessimism but the correct accounting of capacity nothing currently
  running can use. Under this correct model, any K8S+ advantage over
  Batch is a lower bound on the real-world gain, not an upper bound.

The solver is called at every discrete event:
  - Job placed on node
  - Job departs node
  - Phase transition (io_wait changes per workhorse stage)

Between events, boost allocations are constant.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class JobCPUState:
    """CPU state for one job on a node at a point in time."""
    job_id:          str
    soft_cpu:        int      # guaranteed reservation (scheduler signal)
    hard_cpu:        int      # burst ceiling (declared quota)
    io_wait:         float    # current stage io_wait fraction
    stage_threads:   int = 0  # physical thread count for current stage;
                              # bounds how much surplus this job can win in
                              # the greedy auction. 0 = unconstrained (caller
                              # has no per-stage info; use hard_cpu as ceiling)
    boost_alloc:     float = 0.0   # set by solver: soft + boost grant
    effective_vcpu:  float = 0.0   # boost_alloc × (1 - io_wait)
    wasted_vcpu:     float = 0.0   # returned cycles that went nowhere


@dataclass
class NodeCPUResult:
    """Output of one solver run."""
    jobs:               list[JobCPUState]
    total_effective:    float   # Σ effective_vcpu across all jobs
    total_wasted:       float   # cycles permanently lost this interval
    surplus_exhausted:  bool    # True if surplus ran out before all jobs boosted
    hard_limit_waste:   float   # waste due to all boosted jobs at hard limit
    io_ineligible_waste: float  # waste due to io_wait ineligibility (Option 2)


def solve_cpu_boost(
    jobs: list[JobCPUState],
    node_physical_vcpu: int,
) -> NodeCPUResult:
    """
    Run the Option 2 CPU boost solver for one node.

    Args:
        jobs: list of JobCPUState for every job currently on this node
        node_physical_vcpu: total vCPU capacity of the node

    Returns:
        NodeCPUResult with per-job boost allocations and waste accounting
    """
    if not jobs:
        return NodeCPUResult(
            jobs=[], total_effective=0.0, total_wasted=0.0,
            surplus_exhausted=False, hard_limit_waste=0.0,
            io_ineligible_waste=0.0,
        )

    # Step 1: soft allocations (guaranteed)
    total_soft = sum(j.soft_cpu for j in jobs)
    unreserved  = max(0.0, node_physical_vcpu - total_soft)

    # Step 2: surplus = unreserved + io_wait returns at soft allocation
    io_returned_at_soft = sum(j.soft_cpu * j.io_wait for j in jobs)
    surplus = unreserved + io_returned_at_soft

    # Step 3: sort ascending by io_wait (lowest io_wait = highest consumption)
    sorted_jobs = sorted(jobs, key=lambda j: j.io_wait)

    # Step 4: greedy distribution
    remaining_surplus = surplus
    for job in sorted_jobs:
        headroom = job.hard_cpu - job.soft_cpu
        # A job cannot win more of the shared surplus than its CURRENT
        # stage can use, regardless of its lifetime-peak hard_cpu ceiling.
        # Capping here — rather than only measuring the overshoot after
        # the fact — lets the unused remainder fall through to the next
        # job in the greedy order instead of being locked into a grant
        # this job has no way to act on. stage_threads == 0 means the
        # caller has no per-stage info; fall back to hard_cpu-only headroom.
        if job.stage_threads > 0:
            headroom = min(headroom, max(0.0, job.stage_threads - job.soft_cpu))
        # Cap grant so boost_alloc never exceeds the node's physical vCPU.
        # Without this, a solo job's own io_wait soft-returns inflate the
        # surplus and recycle back to the same job, producing boost_alloc > physical.
        max_physical_headroom = max(0.0, node_physical_vcpu - job.soft_cpu)
        grant    = min(headroom, max(0.0, remaining_surplus), max_physical_headroom)
        job.boost_alloc    = job.soft_cpu + grant
        job.effective_vcpu = job.boost_alloc * (1.0 - job.io_wait)
        # Option 2: do NOT add grant * io_wait back to surplus
        # The returned cycles are ineligible for redistribution
        remaining_surplus -= grant
        if remaining_surplus <= 0:
            remaining_surplus = 0.0
            break

    # Jobs not reached by the loop keep boost_alloc = 0 → set to soft
    for job in sorted_jobs:
        if job.boost_alloc == 0.0:
            job.boost_alloc    = job.soft_cpu
            job.effective_vcpu = job.soft_cpu * (1.0 - job.io_wait)

    # Step 5: account for waste
    # io_ineligible_waste: cycles returned by io_wait that couldn't be reused
    io_ineligible_waste = sum(
        j.boost_alloc * j.io_wait for j in jobs
    )
    # hard_limit_waste: surplus that remained after every job hit its
    # hard_cpu ceiling, its current-stage thread ceiling, or ran the
    # auction dry — capacity nothing currently running can use
    hard_limit_waste = remaining_surplus

    total_wasted = io_ineligible_waste + hard_limit_waste
    total_effective = sum(j.effective_vcpu for j in jobs)

    for job in jobs:
        job.wasted_vcpu = job.boost_alloc * job.io_wait

    return NodeCPUResult(
        jobs=jobs,
        total_effective=round(total_effective, 4),
        total_wasted=round(total_wasted, 4),
        surplus_exhausted=(remaining_surplus == 0.0 and
                           any(j.boost_alloc < j.hard_cpu for j in jobs)),
        hard_limit_waste=round(hard_limit_waste, 4),
        io_ineligible_waste=round(io_ineligible_waste, 4),
    )
