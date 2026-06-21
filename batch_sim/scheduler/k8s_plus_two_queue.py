"""
BSIM-54: K8S+ Two-Queue Scheduler.

Extends K8SPlusScheduler with advantage-ratio-based job routing:
  - Queue 1 (q1): advantage_ratio >= k  →  bin-packing, standard panic threshold
  - Queue 2 (q2): advantage_ratio <  k  →  near-capacity, longer panic threshold

Each queue maintains its own node pool and job heap. Nodes are tagged to their
queue and never shared across queues. Cost is tracked per queue independently.

New SchedulerConfig fields used:
  advantage_k: float              threshold for Q1/Q2 split (sweep variable)
  queue2_panic_multiplier: float  Q2 panic = base_panic * multiplier (default 3.0)
"""

from __future__ import annotations

import heapq
import random
import uuid
from typing import Optional

import simpy

from batch_sim.core.engine import NodeModel, JobQueue, Priority, QueueEntry, run_job_process
from batch_sim.core.schemas import SchedulerConfig
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import (
    MetricsCollector, NodeState as NodeStateEnum, PhaseID, EventType, SimEvent
)
from batch_sim.registry.instance_registry import (
    InstanceRegistry, NodeCostAccruer, compute_k8s_capacity
)
from batch_sim.scheduler.queue_router import (
    assign_queue, QueueClass, QueueAssignment, queue_summary
)
from batch_sim.scheduler.burst_pool import NodeBurstPool


# ---------------------------------------------------------------------------
# Burst-pool aware job runner
# ---------------------------------------------------------------------------

def run_job_burst(
    env, job, node, metrics, burst_pool: NodeBurstPool,
    arrival_time, queue_entry_time, scheduler,
):
    """Four-phase job runner with NodeBurstPool gating Phase 2."""
    p = job.profile
    jid = job.job_id
    start_time = env.now
    queue_wait_s = start_time - queue_entry_time

    metrics.job_start(env.now, jid, job.centroid_id, node.node_id)

    # Phase 1 — Download
    metrics.phase_transition(env.now, jid, PhaseID.DOWNLOAD, node.node_id)
    node.add_job(job, PhaseID.DOWNLOAD, ram_gb=p.download_ram_gb, vcpu=1.0)
    yield env.timeout(p.download_duration_s)

    # Phase 2 — Pre-process (burst-pool gated)
    burst_wait_start = env.now
    yield from burst_pool.acquire(p.preprocess_peak_ram_gb)
    burst_wait_s = env.now - burst_wait_start

    metrics.phase_transition(env.now, jid, PhaseID.PREPROCESS, node.node_id)
    node.update_phase(jid, PhaseID.PREPROCESS,
                      ram_gb=p.preprocess_peak_ram_gb, vcpu=p.preprocess_vcpu)
    yield env.timeout(p.preprocess_duration_s)
    burst_pool.release(p.preprocess_peak_ram_gb)

    # Phase 3 — Workhorse
    node.update_phase(jid, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb, vcpu=0.0)
    metrics.phase_transition(env.now, jid, PhaseID.WORKHORSE, node.node_id)
    for stage in p.stages:
        node.update_phase(jid, PhaseID.WORKHORSE,
                          ram_gb=p.workhorse_ram_gb, vcpu=stage.effective_threads)
        yield env.timeout(stage.wall_clock_seconds)

    # Phase 4 — Upload
    metrics.phase_transition(env.now, jid, PhaseID.UPLOAD, node.node_id)
    node.update_phase(jid, PhaseID.UPLOAD, ram_gb=p.upload_ram_gb, vcpu=1.0)
    yield env.timeout(p.upload_duration_s)

    node.remove_job(jid)
    metrics.job_complete(
        t=env.now, job_id=jid, centroid_id=job.centroid_id,
        node_id=node.node_id, queue_wait_s=queue_wait_s,
        total_elapsed_s=env.now - arrival_time,
        retry_count=job.retry_count,
    )
    # Record burst wait for aggregation
    metrics.record(SimEvent(EventType.COST_SAMPLE, env.now, {
        'type': 'burst_wait', 'job_id': jid, 'queue': scheduler.job_queue_class.get(jid, '?'),
        'node_id': node.node_id, 'wait_s': round(burst_wait_s, 2),
    }))
    scheduler.on_job_complete(env, node, job)


# ---------------------------------------------------------------------------
# Single queue pool (reusable for Q1 and Q2)
# ---------------------------------------------------------------------------

class _QueuePool:
    """One scheduling pool: a job queue, a set of nodes, and panic monitors."""

    def __init__(
        self,
        env_ref,
        queue_class: QueueClass,
        panic_threshold_s: float,
        cfg: SchedulerConfig,
        registry: InstanceRegistry,
        metrics: MetricsCollector,
        rng: random.Random,
        centroid_peak_rams: list[float],
        parent,   # K8SPlusTwoQueueScheduler
    ) -> None:
        self.qc = queue_class
        self.panic_threshold_s = panic_threshold_s
        self.cfg = cfg
        self.registry = registry
        self.metrics = metrics
        self.rng = rng
        self.centroid_peak_rams = centroid_peak_rams
        self.parent = parent

        self._queue = JobQueue()
        self._nodes: dict[str, NodeModel] = {}
        self._burst_pools: dict[str, NodeBurstPool] = {}
        self._accruers: dict[str, NodeCostAccruer] = {}
        self._reserved: dict[str, str] = {}
        self._capacity_cache: dict[str, object] = {}
        self._panic_monitors: dict[str, object] = {}

    def _k8s_capacity(self, instance):
        if instance.name not in self._capacity_cache:
            # BSIM-104: compute_k8s_capacity takes a scalar spike_max_gb. This legacy
            # two-queue scheduler derives it from the observed centroid peaks, matching
            # K8SScheduler's no-tier path (largest positive peak).
            fitting = [r for r in self.centroid_peak_rams if r > 0]
            spike_max = max(fitting) if fitting else 0.0
            self._capacity_cache[instance.name] = compute_k8s_capacity(
                instance=instance,
                spike_max_gb=spike_max,
                os_overhead_gb=self.cfg.os_overhead_gb,
            )
        return self._capacity_cache[instance.name]

    def enqueue(self, env, job, assignment: QueueAssignment, arrival_time):
        self.metrics.job_queued(env.now, job.job_id, job.centroid_id,
                                f"NORMAL_{self.qc.value.upper()}")
        self._queue.enqueue(job, arrival_time=arrival_time,
                            priority=Priority.NORMAL, enqueue_time=env.now)
        proc = env.process(self._panic_monitor(env, job, enqueue_time=env.now,
                                               preferred_instance=assignment.instance))
        self._panic_monitors[job.job_id] = proc
        self._try_schedule(env)

    def on_job_complete(self, env, node, job):
        soft = job.profile.soft_limit_ram_gb
        node.allocated_ram_gb = max(0.0, node.allocated_ram_gb - soft)
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - job.profile.workhorse_declared_vcpu)
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        if node.job_count == 0:
            node.state = NodeStateEnum.IDLE
            node.idle_since = env.now
            self.metrics.node_idle(env.now, node.node_id)
            env.process(self._idle_timer(env, node))
        self._try_schedule(env)

    def guarantee_capacity(self, env, job, preferred_instance=None):
        soft = job.profile.soft_limit_ram_gb
        vcpu = job.profile.workhorse_declared_vcpu
        for node in self._nodes.values():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node.node_id not in self._reserved
                    and self._fits(node, soft, vcpu)):
                self._reserved[node.node_id] = job.job_id
                return
        instance = preferred_instance or self.registry.cheapest_fitting(
            min_ram_gb=job.profile.peak_ram_gb + self.cfg.os_overhead_gb,
            min_vcpu=vcpu,
        )
        if instance:
            env.process(self._launch_node(env, instance, for_job=job))

    def _try_schedule(self, env):
        changed = True
        while changed and self._queue:
            changed = False
            for entry in sorted(self._queue._heap):
                if self._place_job(env, entry):
                    self._queue._heap = [e for e in self._queue._heap
                                         if e.job.job_id != entry.job.job_id]
                    heapq.heapify(self._queue._heap)
                    changed = True
                    break

    def _place_job(self, env, entry):
        job = entry.job
        p = job.profile
        best = self._best_fit_node(p.soft_limit_ram_gb, p.workhorse_declared_vcpu, job.job_id)
        if best is None:
            return False
        mon = self._panic_monitors.pop(job.job_id, None)
        if mon and mon.is_alive:
            mon.interrupt("placed")
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += p.soft_limit_ram_gb
        best.allocated_vcpu += p.workhorse_declared_vcpu
        if best.state == NodeStateEnum.IDLE:
            best.state = NodeStateEnum.READY

        # Update burst pool for this node based on this job's actual peak
        bp = self._burst_pools[best.node_id]
        bp.update_max_peak(p.preprocess_peak_ram_gb)

        env.process(run_job_burst(
            env=env, job=job, node=best, metrics=self.metrics,
            burst_pool=bp,
            arrival_time=entry.arrival_time,
            queue_entry_time=entry.enqueue_time,
            scheduler=self.parent,
        ))
        return True

    def _fits(self, node, soft_gb, vcpu):
        cap = self._capacity_cache.get(node.instance.name)
        if cap is None or cap.effective_schedulable_gb <= 0:
            return False
        return (node.allocated_ram_gb + soft_gb <= cap.effective_schedulable_gb
                and node.allocated_vcpu + vcpu <= node.physical_vcpu)

    def _best_fit_node(self, soft_gb, vcpu, job_id):
        candidates = [
            (node.allocated_ram_gb, node)
            for node in self._nodes.values()
            if node.state == NodeStateEnum.READY
            and self._reserved.get(node.node_id, job_id) == job_id
            and self._fits(node, soft_gb, vcpu)
        ]
        return max(candidates, key=lambda x: x[0])[1] if candidates else None

    def _launch_node(self, env, instance, for_job=None):
        node_id = str(uuid.uuid4())[:8]
        self.metrics.node_launching(env.now, node_id, instance.name)
        cap = self._k8s_capacity(instance)
        self._capacity_cache[instance.name] = cap
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.cfg.os_overhead_gb)
        self._nodes[node_id] = node
        self._burst_pools[node_id] = NodeBurstPool(
            env=env,
            node_physical_ram_gb=instance.ram_gb,
            os_overhead_gb=self.cfg.os_overhead_gb,
        )
        self._accruers[node_id] = NodeCostAccruer(
            node_id=node_id, instance=instance, launch_time=env.now)
        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if for_job:
            self._reserved[node_id] = for_job.job_id
        self._try_schedule(env)

    def _panic_monitor(self, env, job, enqueue_time, preferred_instance=None):
        try:
            yield env.timeout(self.panic_threshold_s)
        except simpy.Interrupt:
            return
        self.metrics.panic_trigger(env.now, job.job_id, env.now - enqueue_time)
        self._queue.elevate_to_urgent(job.job_id)
        self.guarantee_capacity(env, job, preferred_instance=preferred_instance)

    def _idle_timer(self, env, node):
        idle_start = env.now
        yield env.timeout(self.cfg.idle_timeout_seconds)
        if node.state == NodeStateEnum.IDLE and node.job_count == 0:
            node.state = NodeStateEnum.TERMINATED
            self.metrics.node_terminated(env.now, node.node_id, env.now - idle_start)
            accruer = self._accruers.get(node.node_id)
            if accruer:
                accruer.terminate(env.now)

    def finalize(self, env):
        for a in self._accruers.values():
            if not a.is_terminated:
                a.terminate(env.now)

    @property
    def accruers(self):
        return list(self._accruers.values())


# ---------------------------------------------------------------------------
# Two-queue scheduler
# ---------------------------------------------------------------------------

class K8SPlusTwoQueueScheduler:
    """
    K8S+ scheduler with advantage-ratio queue routing and per-queue node pools.

    Jobs are classified at arrival using compute_advantage_ratio(M, S, C):
      advantage_ratio >= k  →  Queue 1 (q1): bin-packing, standard panic
      advantage_ratio <  k  →  Queue 2 (q2): near-capacity, longer panic

    Each queue has its own pool of nodes; nodes are never shared across queues.
    """

    def __init__(
        self,
        cfg: SchedulerConfig,
        registry: InstanceRegistry,
        metrics: MetricsCollector,
        centroid_peak_rams: list[float],
        k: float = 4.0,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.cfg = cfg
        self.registry = registry
        self.metrics = metrics
        self.k = k
        self.rng = rng or random.Random(42)
        self.centroid_peak_rams = centroid_peak_rams

        q2_panic = cfg.panic_threshold_seconds * getattr(cfg, 'queue2_panic_multiplier', 3.0)

        self._q1 = _QueuePool(None, QueueClass.Q1, cfg.panic_threshold_seconds,
                              cfg, registry, metrics, self.rng, centroid_peak_rams, self)
        self._q2 = _QueuePool(None, QueueClass.Q2, q2_panic,
                              cfg, registry, metrics, self.rng, centroid_peak_rams, self)

        # job_id → QueueClass for burst-wait attribution
        self.job_queue_class: dict[str, str] = {}
        # job_id → QueueAssignment for reporting
        self._assignments: dict[str, QueueAssignment] = {}

    def on_job_arrival(self, env, job, arrival_time):
        p = job.profile
        assignment = assign_queue(
            peak_ram_gb=p.preprocess_peak_ram_gb,
            steady_ram_gb=p.preprocess_steady_ram_gb,
            registry=self.registry,
            k=self.k,
        )
        if assignment is None:
            # Job exceeds all available instances — record and discard
            self.metrics.job_terminal(env.now, job.job_id, job.centroid_id)
            return

        self._assignments[job.job_id] = assignment
        self.job_queue_class[job.job_id] = assignment.queue.value

        pool = self._q1 if assignment.queue == QueueClass.Q1 else self._q2
        pool.enqueue(env, job, assignment, arrival_time)

    def on_job_complete(self, env, node, job):
        # Route completion to the correct pool
        qc = self.job_queue_class.get(job.job_id, QueueClass.Q1.value)
        pool = self._q1 if qc == QueueClass.Q1.value else self._q2
        pool.on_job_complete(env, node, job)

    def guarantee_capacity(self, env, job, preferred_instance=None):
        qc = self.job_queue_class.get(job.job_id, QueueClass.Q1.value)
        pool = self._q1 if qc == QueueClass.Q1.value else self._q2
        pool.guarantee_capacity(env, job, preferred_instance=preferred_instance)

    @property
    def accruers(self):
        return self._q1.accruers + self._q2.accruers

    def finalize(self, env):
        self._q1.finalize(env)
        self._q2.finalize(env)

    def capacity_report(self):
        return {
            'q1': {name: {'headroom_pct': cap.headroom_pct,
                          'effective_schedulable_gb': cap.effective_schedulable_gb}
                   for name, cap in self._q1._capacity_cache.items()},
            'q2': {name: {'headroom_pct': cap.headroom_pct,
                          'effective_schedulable_gb': cap.effective_schedulable_gb}
                   for name, cap in self._q2._capacity_cache.items()},
        }

    def queue_assignment_report(self) -> dict:
        assignments = list(self._assignments.values())
        return {
            'k': self.k,
            'summary': queue_summary(assignments),
            'q1_accruers': len(self._q1.accruers),
            'q2_accruers': len(self._q2.accruers),
            'q1_cost': round(sum(a.total_cost_usd for a in self._q1.accruers if a.is_terminated), 2),
            'q2_cost': round(sum(a.total_cost_usd for a in self._q2.accruers if a.is_terminated), 2),
        }

    def burst_wait_stats(self) -> dict:
        waits = [
            e.data['wait_s']
            for e in self.metrics.log
            if e.event_type == EventType.COST_SAMPLE
            and e.data.get('type') == 'burst_wait'
        ]
        if not waits:
            return {'count': 0, 'mean': 0.0, 'max': 0.0, 'nonzero_count': 0}
        nonzero = [w for w in waits if w > 0.1]
        import statistics
        return {
            'count': len(waits),
            'nonzero_count': len(nonzero),
            'mean': round(statistics.mean(waits), 2),
            'max': round(max(waits), 2),
            'mean_nonzero': round(statistics.mean(nonzero), 2) if nonzero else 0.0,
        }
