"""
BSIM-72: CPU boost solver integration.

Provides run_cpu_boost_sample() — called at each phase transition by both
schedulers to:
  1. Snapshot current jobs and their stage io_wait
  2. Run the appropriate solver (K8S Option 2 or Batch proportional CFS)
  3. Emit a CPU_WASTE event with the waste breakdown
  4. Return effective_vcpu per job for utilization tracking

Called at:
  - JOB_START (job enters node)
  - PHASE_TRANSITION (io_wait changes between stages)
  - JOB_COMPLETE / JOB_CRASH (job leaves node, reduce pool)
"""

from __future__ import annotations

from typing import Any

from batch_sim.metrics.collector import MetricsCollector, EventType, SimEvent
from batch_sim.scheduler.cpu_boost_solver import (
    JobCPUState, solve_cpu_boost, NodeCPUResult
)
from batch_sim.core.engine import NodeModel


def _get_stage_io_wait(slot: Any) -> float:
    """
    Return the io_wait fraction for the job's current phase.

    Non-workhorse phases (download, preprocess, upload) are single-threaded
    even though the job holds a multi-vCPU soft_cpu reservation.  Returning
    0.0 here would tell the Option-2 solver "this job is maximally CPU-
    efficient" and give it first priority for surplus, starving workhorse
    jobs of boost allocation while the download/upload job can only use 1
    thread anyway.

    Instead we model non-workhorse phases with io_wait = 1 - 1/soft_cpu,
    reflecting that only 1 of soft_cpu declared threads is actually active.
    This correctly deprioritises them in the surplus auction without
    removing their base soft allocation.
    """
    from batch_sim.metrics.collector import PhaseID
    if slot.current_phase != PhaseID.WORKHORSE:
        job = slot.job
        soft = max(1, getattr(job, 'soft_cpu', 0) or 1)
        return 1.0 - 1.0 / soft
    # Use the stage-level io_wait if available via the job spec
    job = slot.job
    if hasattr(job, 'profile') and job.profile.stages:
        # Find which stage is currently running based on stage ordering
        # (simplified: use the mean io_wait across parallel stages)
        waits = []
        for s in job.profile.stages:
            if s.index % 2 == 0:   # parallel stage
                eff = s.effective_threads
                dec = s.declared_threads
                if dec > 0:
                    waits.append(1.0 - eff / dec)
        if waits:
            return sum(waits) / len(waits)
    return 0.0


def _get_stage_threads(slot: Any) -> int:
    """Return declared thread count for the job's current phase."""
    from batch_sim.metrics.collector import PhaseID
    if slot.current_phase == PhaseID.WORKHORSE:
        job = slot.job
        if hasattr(job, 'profile'):
            return job.profile.workhorse_declared_vcpu or job.soft_cpu or 1
    return 1   # download / preprocess / upload are single-threaded


def run_cpu_boost_k8s(
    env: Any,
    node: NodeModel,
    metrics: MetricsCollector,
    scheduler_type: str = 'k8s',
) -> None:
    """
    Run the K8S Option 2 CPU boost solver for one node at the current
    simulated time and emit a CPU_WASTE event.

    Called at every phase transition on the node.
    """
    slots = list(node._slots.values())
    if not slots:
        return

    jobs_state = []
    for slot in slots:
        job    = slot.job
        soft   = getattr(job, 'soft_cpu', 0) or node.physical_vcpu // max(len(slots), 1)
        hard   = getattr(job, 'hard_cpu', 0) or soft
        io_w   = _get_stage_io_wait(slot)
        threads= _get_stage_threads(slot)
        jobs_state.append(JobCPUState(
            job_id=job.job_id,
            soft_cpu=max(1, soft),
            hard_cpu=max(1, hard),
            io_wait=io_w,
            stage_threads=threads,
        ))

    result = solve_cpu_boost(jobs_state, int(node.physical_vcpu))

    # Write effective_vcpu back, fire cpu_change_events, and emit per-job CPU_WASTE.
    for slot, js in zip(slots, result.jobs):
        slot.effective_vcpu = js.effective_vcpu
        evt = slot.cpu_change_event
        if evt is not None and not evt.triggered:
            slot.cpu_change_event = None
            evt.succeed()

        thread_ceil = getattr(js, 'stage_threads', 0) or js.hard_cpu
        per_job_thread_waste = max(0.0, js.boost_alloc - thread_ceil) * (1.0 - js.io_wait)
        vcpu_cap = slot.stage_vcpu_cap if slot.stage_vcpu_cap > 0 else js.effective_vcpu
        current_vcpu = min(js.effective_vcpu, vcpu_cap)
        metrics.record(SimEvent(EventType.CPU_WASTE, env.now, {
            'job_id':              js.job_id,
            'node_id':             node.node_id,
            'scheduler':           scheduler_type,
            'phase':               slot.current_phase.value,
            'effective_vcpu':      round(js.effective_vcpu, 3),
            'current_vcpu':        round(current_vcpu, 3),
            'boost_alloc':         round(js.boost_alloc, 3),
            'io_ineligible_waste': round(js.boost_alloc * js.io_wait, 3),
            'thread_count_waste':  round(per_job_thread_waste, 3),
            'hard_limit_waste':    0.0,
            'remaining_cpu_s':     round(slot.remaining_cpu_s, 3),
        }))

    _emit_cpu_waste(env, node, result, metrics, scheduler_type)


def run_cpu_boost_batch(
    env: Any,
    node: NodeModel,
    metrics: MetricsCollector,
) -> None:
    """
    Run the Batch proportional CFS solver for one node at the current
    simulated time and emit a CPU_WASTE event.

    Batch uses cpu.shares proportional weighting with no hard quota ceiling.
    Surplus is redistributed among unsaturated jobs (those below thread count)
    until all surplus is absorbed or all jobs are thread-saturated.
    """
    slots = list(node._slots.values())
    if not slots:
        return

    # Build job states — for Batch, hard_cpu is effectively unlimited
    # (no cfs_quota), so we use stage thread count as the physical ceiling
    class _BatchJob:
        def __init__(self, job_id, soft_cpu, stage_threads, io_wait):
            self.job_id        = job_id
            self.soft_cpu      = soft_cpu
            self.stage_threads = stage_threads
            self.io_wait       = io_wait
            self.boost_alloc   = 0.0
            self.effective_vcpu= 0.0
            self.thread_waste  = 0.0

    jobs = []
    for slot in slots:
        job     = slot.job
        soft    = getattr(job, 'soft_cpu', 0) or node.physical_vcpu // max(len(slots), 1)
        threads = _get_stage_threads(slot)
        io_w    = _get_stage_io_wait(slot)
        jobs.append(_BatchJob(job.job_id, max(1, soft), threads, io_w))

    # Iterative proportional CFS solver
    remaining   = float(node.physical_vcpu)
    unsaturated = list(jobs)

    while unsaturated and remaining > 1e-6:
        total_shares    = sum(j.soft_cpu for j in unsaturated)
        newly_saturated = []

        for j in unsaturated:
            alloc = remaining * (j.soft_cpu / total_shares)
            if alloc >= j.stage_threads:
                j.boost_alloc = j.stage_threads
                newly_saturated.append(j)
            else:
                j.boost_alloc = alloc

        distributed   = sum(j.boost_alloc for j in unsaturated)
        remaining    -= distributed
        if remaining < 0:
            remaining = 0.0

        if not newly_saturated:
            break
        unsaturated = [j for j in unsaturated if j not in newly_saturated]

    # Finalise unsaturated jobs at their current boost_alloc
    # (remaining surplus after all saturated == thread_count_waste)
    for j in jobs:
        j.effective_vcpu = j.boost_alloc * (1.0 - j.io_wait)
        j.thread_waste   = max(0.0, j.boost_alloc - j.stage_threads) * (1.0 - j.io_wait)

    thread_waste_total = sum(j.thread_waste for j in jobs)
    effective_total    = sum(j.effective_vcpu for j in jobs)

    # Write effective_vcpu back, fire cpu_change_events, and emit per-job CPU_WASTE.
    for slot, bj in zip(slots, jobs):
        slot.effective_vcpu = bj.effective_vcpu
        evt = slot.cpu_change_event
        if evt is not None and not evt.triggered:
            slot.cpu_change_event = None
            evt.succeed()

        vcpu_cap = slot.stage_vcpu_cap if slot.stage_vcpu_cap > 0 else bj.effective_vcpu
        current_vcpu = min(bj.effective_vcpu, vcpu_cap)
        metrics.record(SimEvent(EventType.CPU_WASTE, env.now, {
            'job_id':              bj.job_id,
            'node_id':             node.node_id,
            'scheduler':           'batch',
            'phase':               slot.current_phase.value,
            'effective_vcpu':      round(bj.effective_vcpu, 3),
            'current_vcpu':        round(current_vcpu, 3),
            'boost_alloc':         round(bj.boost_alloc, 3),
            'io_ineligible_waste': round(bj.boost_alloc * bj.io_wait, 3),
            'thread_count_waste':  round(bj.thread_waste, 3),
            'hard_limit_waste':    0.0,
            'remaining_cpu_s':     round(slot.remaining_cpu_s, 3),
        }))

    # Node-level composite (no job_id → node scope)
    metrics.record(SimEvent(EventType.CPU_WASTE, env.now, {
        'node_id':             node.node_id,
        'scheduler':           'batch',
        'job_count':           len(jobs),
        'effective_vcpu':      round(effective_total, 3),
        'thread_count_waste':  round(thread_waste_total, 3),
        'io_ineligible_waste': 0.0,
        'hard_limit_waste':    0.0,
        'total_waste':         round(thread_waste_total, 3),
    }))


def _emit_cpu_waste(
    env: Any,
    node: NodeModel,
    result: NodeCPUResult,
    metrics: MetricsCollector,
    scheduler_type: str,
) -> None:
    """Emit a node-level composite CPU_WASTE event (no job_id → node scope)."""
    thread_waste = sum(
        max(0.0, j.boost_alloc - (getattr(j, 'stage_threads', 0) or j.hard_cpu)) * (1.0 - j.io_wait)
        for j in result.jobs
    )

    metrics.record(SimEvent(EventType.CPU_WASTE, env.now, {
        'node_id':             node.node_id,
        'scheduler':           scheduler_type,
        'job_count':           len(result.jobs),
        'effective_vcpu':      round(result.total_effective, 3),
        'io_ineligible_waste': round(result.io_ineligible_waste, 3),
        'hard_limit_waste':    round(result.hard_limit_waste, 3),
        'thread_count_waste':  round(thread_waste, 3),
        'total_waste':         round(
            result.io_ineligible_waste + result.hard_limit_waste + thread_waste, 3),
    }))
