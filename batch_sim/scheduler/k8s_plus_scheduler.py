"""
BSIM-50: K8S+ / OKD+ Scheduler with DaemonSet semaphore facility.

Models a third scheduling strategy where a node-local semaphore (backed by a
Kubernetes DaemonSet sidecar in production) serialises jobs through Phase 2
(the peak-RAM preprocess phase).  Rather than crashing and requeuing on memory
collision, jobs block on the semaphore, wait for a permit, execute Phase 2
safely, then release.  Zero crashes; some semaphore-wait latency instead.

Concurrency limit per node:
  burst_slots = floor(headroom_gb / tier_local_MM)
  where headroom_gb = effective_schedulable_gb  (physical - os - spike_headroom)
  semaphore permits = max(1, burst_slots)   # always at least 1 (mutex)

New metric: SEMAPHORE_WAIT — emitted with job_id, node_id, wait_s.
"""

from __future__ import annotations

import uuid, random
from typing import Optional

import simpy

from batch_sim.core.engine import (
    NodeModel, JobQueue, Priority, OverloadHandler, run_job_process
)
from batch_sim.core.schemas import SchedulerConfig
from batch_sim.generator.job_spec import JobSpec, PhaseProfile
from batch_sim.metrics.collector import (
    MetricsCollector, NodeState as NodeStateEnum, PhaseID, EventType, SimEvent
)
from batch_sim.registry.instance_registry import (
    InstanceRegistry, NodeCostAccruer, compute_k8s_capacity
)
from batch_sim.core.schemas import InstanceTypeConfig


# ---------------------------------------------------------------------------
# Semaphore primitive
# ---------------------------------------------------------------------------

class NodeSemaphore:
    """
    SimPy-based counting semaphore for Phase-2 concurrency control.
    acquire() is a generator that yields until a permit is available.
    """

    def __init__(self, env: simpy.Environment, permits: int) -> None:
        self._env = env
        self._permits = permits
        self._available = permits
        self._waiters: list[simpy.Event] = []

    @property
    def permits(self) -> int:
        return self._permits

    @property
    def available(self) -> int:
        return self._available

    def acquire(self, job_id: str):
        """Generator: yields until a permit is granted."""
        if self._available > 0:
            self._available -= 1
            return
        # Block until released
        event = self._env.event()
        self._waiters.append(event)
        yield event
        self._available -= 1

    def release(self) -> None:
        if self._waiters:
            waiter = self._waiters.pop(0)
            waiter.succeed()
        else:
            self._available = min(self._available + 1, self._permits)


# ---------------------------------------------------------------------------
# Semaphore-aware job process
# ---------------------------------------------------------------------------

def run_job_process_plus(
    env, job, node, metrics, sem: NodeSemaphore,
    arrival_time, queue_entry_time, scheduler,
):
    """Phase 2 is gated by the node semaphore — no crash possible."""
    p = job.profile
    job_id = job.job_id
    start_time = env.now
    queue_wait_s = start_time - queue_entry_time

    metrics.job_start(env.now, job_id, job.centroid_id, node.node_id)

    # Phase 1: Download
    metrics.phase_transition(env.now, job_id, PhaseID.DOWNLOAD, node.node_id)
    node.add_job(job, PhaseID.DOWNLOAD, ram_gb=p.download_ram_gb, vcpu=1.0)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.download_duration_s)

    # Phase 2: Pre-process — acquire semaphore first
    sem_wait_start = env.now
    yield env.process(_acquire_sem(env, sem, job_id))
    sem_wait_s = env.now - sem_wait_start

    if sem_wait_s > 0.1:
        metrics.record(SimEvent(EventType.PHASE_TRANSITION, env.now, {
            'job_id': job_id, 'phase': 'semaphore_wait',
            'node_id': node.node_id, 'wait_s': round(sem_wait_s, 2)
        }))

    metrics.phase_transition(env.now, job_id, PhaseID.PREPROCESS, node.node_id)
    node.update_phase(job_id, PhaseID.PREPROCESS,
                      ram_gb=p.preprocess_peak_ram_gb, vcpu=p.preprocess_vcpu)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.preprocess_duration_s)
    sem.release()

    # Phase 3: Workhorse
    node.update_phase(job_id, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb, vcpu=0.0)
    metrics.phase_transition(env.now, job_id, PhaseID.WORKHORSE, node.node_id)
    for stage in p.stages:
        node.update_phase(job_id, PhaseID.WORKHORSE,
                          ram_gb=p.workhorse_ram_gb, vcpu=stage.effective_threads)
        scheduler.cpu_boost(env, node, metrics)
        yield env.timeout(stage.wall_clock_seconds)

    # Phase 4: Upload
    metrics.phase_transition(env.now, job_id, PhaseID.UPLOAD, node.node_id)
    node.update_phase(job_id, PhaseID.UPLOAD, ram_gb=p.upload_ram_gb, vcpu=1.0)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.upload_duration_s)

    node.remove_job(job_id)
    metrics.job_complete(
        t=env.now, job_id=job_id, centroid_id=job.centroid_id,
        node_id=node.node_id, queue_wait_s=queue_wait_s,
        total_elapsed_s=env.now - arrival_time,
        retry_count=job.retry_count,
    )
    # Store semaphore wait for aggregation (piggyback on data dict via a custom event)
    metrics.record(SimEvent(EventType.COST_SAMPLE, env.now, {
        'type': 'semaphore_wait', 'job_id': job_id,
        'node_id': node.node_id, 'wait_s': round(sem_wait_s, 2)
    }))
    scheduler.on_job_complete(env, node, job)


def _acquire_sem(env, sem, job_id):
    """Wrapper generator so SimPy can process the acquire."""
    if sem.available > 0:
        sem.acquire(job_id)
        return
    yield from sem.acquire(job_id)


# ---------------------------------------------------------------------------
# K8S+ Scheduler
# ---------------------------------------------------------------------------

class K8SPlusScheduler:
    """
    K8S scheduler with per-node semaphore gating Phase 2.
    Inherits all packing and panic logic from the K8S strategy;
    overrides the job runner to use the semaphore-aware process.
    """

    def __init__(self, cfg, registry, metrics, centroid_peak_rams, rng=None):
        self.cfg = cfg
        self.registry = registry
        self.metrics = metrics
        self.centroid_peak_rams = centroid_peak_rams
        self.rng = rng or random.Random(42)

        self._queue = JobQueue()
        self._nodes: dict[str, NodeModel] = {}
        self._sems: dict[str, NodeSemaphore] = {}   # node_id → semaphore
        self._accruers: dict[str, NodeCostAccruer] = {}
        self._reserved: dict[str, str] = {}
        self._capacity_cache = {}
        self._panic_monitors = {}
        self._env = None

    def _setup(self, env):
        self._env = env

    def _k8s_capacity(self, instance):
        if instance.name not in self._capacity_cache:
            self._capacity_cache[instance.name] = compute_k8s_capacity(
                instance=instance,
                centroid_peak_rams=self.centroid_peak_rams,
                os_overhead_gb=self.cfg.k8s_os_overhead_gb,
            )
        return self._capacity_cache[instance.name]

    def _sem_permits(self, instance) -> int:
        cap = self._k8s_capacity(instance)
        if cap.soft_limit_gb <= 0 or cap.effective_schedulable_gb <= 0:
            return 1
        # How many simultaneous spikes fit in headroom?
        slots = int(cap.spike_headroom_gb / cap.tier_local_mm_gb) if cap.tier_local_mm_gb > 0 else 1
        return max(1, slots)

    def on_job_arrival(self, env, job, arrival_time):
        if not self._env:
            self._setup(env)
        self.metrics.job_queued(env.now, job.job_id, job.centroid_id, "NORMAL")
        self._queue.enqueue(job, arrival_time=arrival_time,
                            priority=Priority.NORMAL, enqueue_time=env.now)
        proc = env.process(self._panic_monitor(env, job, enqueue_time=env.now))
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

    def guarantee_capacity(self, env, job):
        soft = job.profile.soft_limit_ram_gb
        vcpu = job.profile.workhorse_declared_vcpu
        for node in self._nodes.values():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node.node_id not in self._reserved
                    and self._k8s_fits(node, soft, vcpu)):
                self._reserved[node.node_id] = job.job_id
                return
        instance = self.registry.cheapest_fitting(
            min_ram_gb=job.profile.peak_ram_gb + self.cfg.k8s_os_overhead_gb,
            min_vcpu=vcpu,
        )
        if instance:
            env.process(self._launch_node(env, instance, for_job=job))

    def _try_schedule(self, env):
        import heapq
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
        soft = p.soft_limit_ram_gb
        vcpu = p.workhorse_declared_vcpu
        best = self._best_fit_node(soft, vcpu, job.job_id)
        if best is None:
            return False
        mon = self._panic_monitors.pop(job.job_id, None)
        if mon and mon.is_alive:
            mon.interrupt("placed")
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += soft
        best.allocated_vcpu += vcpu
        if best.state == NodeStateEnum.IDLE:
            best.state = NodeStateEnum.READY
        sem = self._sems[best.node_id]
        env.process(run_job_process_plus(
            env=env, job=job, node=best, metrics=self.metrics, sem=sem,
            arrival_time=entry.arrival_time, queue_entry_time=entry.enqueue_time,
            scheduler=self,
        ))
        return True

    def _k8s_fits(self, node, soft_gb, vcpu):
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
            and self._k8s_fits(node, soft_gb, vcpu)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda x: x[0])[1]

    def _launch_node(self, env, instance, for_job=None):
        node_id = str(uuid.uuid4())[:8]
        self.metrics.node_launching(env.now, node_id, instance.name)
        cap = self._k8s_capacity(instance)
        permits = self._sem_permits(instance)
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.cfg.k8s_os_overhead_gb)
        self._nodes[node_id] = node
        self._sems[node_id] = NodeSemaphore(env, permits)
        self._capacity_cache[instance.name] = cap
        self._accruers[node_id] = NodeCostAccruer(
            node_id=node_id, instance=instance, launch_time=env.now)
        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if for_job:
            self._reserved[node_id] = for_job.job_id
        self._try_schedule(env)

    def _panic_monitor(self, env, job, enqueue_time):
        try:
            yield env.timeout(self.cfg.panic_threshold_seconds)
        except simpy.Interrupt:
            return
        self.metrics.panic_trigger(env.now, job.job_id, env.now - enqueue_time)
        self._queue.elevate_to_urgent(job.job_id)
        self.guarantee_capacity(env, job)

    def _idle_timer(self, env, node):
        idle_start = env.now
        yield env.timeout(self.cfg.idle_timeout_seconds)
        if node.state == NodeStateEnum.IDLE and node.job_count == 0:
            node.state = NodeStateEnum.TERMINATED
            self.metrics.node_terminated(env.now, node.node_id, env.now - idle_start)
            accruer = self._accruers.get(node.node_id)
            if accruer:
                accruer.terminate(env.now)

    def cpu_boost(self, env, node, metrics):
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_k8s
        run_cpu_boost_k8s(env, node, metrics)

    @property
    def accruers(self):
        return list(self._accruers.values())

    def finalize(self, env):
        for accruer in self._accruers.values():
            if not accruer.is_terminated:
                accruer.terminate(env.now)

    def capacity_report(self):
        return {
            name: {
                'tier_local_mm_gb': cap.tier_local_mm_gb,
                'effective_schedulable_gb': cap.effective_schedulable_gb,
                'soft_limit_gb': cap.soft_limit_gb,
                'max_schedulable_jobs': cap.max_schedulable_jobs,
                'headroom_pct': round(cap.headroom_pct, 1),
                'semaphore_permits': self._sem_permits(
                    next(a.instance for a in self._accruers.values()
                         if a.instance.name == name)
                ) if any(a.instance.name == name for a in self._accruers.values()) else '?',
            }
            for name, cap in self._capacity_cache.items()
        }

    def semaphore_wait_stats(self):
        """Extract semaphore wait times from the metrics log."""
        waits = [
            e.data['wait_s']
            for e in self.metrics.log
            if e.event_type == EventType.COST_SAMPLE
            and e.data.get('type') == 'semaphore_wait'
        ]
        if not waits:
            return {'count': 0, 'mean': 0, 'max': 0, 'nonzero_count': 0}
        nonzero = [w for w in waits if w > 0.1]
        import statistics
        return {
            'count': len(waits),
            'nonzero_count': len(nonzero),
            'mean': round(statistics.mean(waits), 2),
            'max': round(max(waits), 2),
            'mean_nonzero': round(statistics.mean(nonzero), 2) if nonzero else 0,
        }
