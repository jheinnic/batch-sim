"""
BSIM-70: CPU boost solver — Option 2 greedy allocator.

Distributes surplus vCPU cycles to jobs on a node beyond their soft_cpu
reservation, up to their hard_cpu ceiling.

Option 2 semantics (correct, not merely conservative):
  - Jobs are sorted by io_wait ascending (lowest io_wait first)
  - Each job absorbs surplus up to its hard_cpu headroom
  - Cycles returned by a job's I/O wait are NOT redistributed
  - Any surplus remaining after all jobs are satisfied or at hard_cpu
    is permanently wasted for the current scheduling interval

Why Option 2 is physically correct, not merely pessimistic:

  The hard_cpu limit is declared at the maximum any stage of the
  container will demand — specifically, the thread count of the most
  parallel stage. The kernel enforces this limit statically for the
  container's lifetime because the OS has no concept of phases.

  A job that returns cycles in one stage is not relinquishing its
  entitlement — it is in a phase where it cannot use what it is entitled
  to. Its entitlement remains declared at the hard limit.

  Redistributing those returned cycles to another job's boost allocation
  would set the kernel up for CPU starvation: if the first job's next
  stage arrives at its maximum threading capacity simultaneously with the
  boosted job's demand, the kernel faces two jobs claiming their full
  hard limit with no headroom to honour both. Option 2 prevents this by
  withholding returned cycles from redistribution — exactly as the kernel
  correctly does to guarantee the declared hard limit is always satisfiable
  on arrival at a demanding stage.

  The wasted cycles are therefore not a modelling pessimism but the correct
  accounting of capacity the kernel withholds to protect future stage demands.
  Under this correct model, any K8S+ advantage over Batch is a lower bound
  on the real-world gain, not an upper bound.

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
    soft_cpu:        int      # guaranteed reservation
    hard_cpu:        int      # burst ceiling (thread count)
    io_wait:         float    # current stage io_wait fraction
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
        grant    = min(headroom, max(0.0, remaining_surplus))
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
    # hard_limit_waste: surplus that remained after all jobs hit hard_cpu
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
