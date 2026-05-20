"""
BSIM-89/90: K8S+ Multi-Pool Scheduler with admin-configured headroom and age cordoning.

Replaces the fixed-permit NodeSemaphore design with:
  - Multi-pool job routing by RAM band (exclusive_min_gb, inclusive_max_gb]
  - Per-pool scheduled_zone = node_ram - os_overhead - daemonset_headroom_gb
  - NodeBurstPool with headroom fixed at daemonset_headroom_gb (not workload-derived)
  - Age-based cordoning: SimPy timer fires at age_cordon_s and marks node as
    no-new-placements; jobs already running complete normally
"""

from __future__ import annotations

import heapq
import random
import uuid
from typing import Optional

import simpy

from batch_sim.core.engine import NodeModel, JobQueue, Priority
from batch_sim.core.schemas import K8SPlusPoolConfig, K8SPlusSchedulerConfig
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import (
    MetricsCollector, NodeState as NodeStateEnum, PhaseID, EventType, SimEvent
)
from batch_sim.registry.instance_registry import InstanceRegistry, NodeCostAccruer
from batch_sim.scheduler.burst_pool import NodeBurstPool


# ---------------------------------------------------------------------------
# Burst-pool-aware job runner (BSIM-89)
# ---------------------------------------------------------------------------

def _run_job(
    env, job, node, metrics, burst_pool: NodeBurstPool,
    arrival_time, queue_entry_time, scheduler,
):
    """Four-phase runner with NodeBurstPool gating Phase 2 and cpu_boost on transitions."""
    p = job.profile
    jid = job.job_id
    queue_wait_s = env.now - queue_entry_time

    metrics.job_start(env.now, jid, job.centroid_id, node.node_id)

    # Phase 1 — Download
    metrics.phase_transition(env.now, jid, PhaseID.DOWNLOAD, node.node_id)
    node.add_job(job, PhaseID.DOWNLOAD, ram_gb=p.download_ram_gb, vcpu=1.0)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.download_duration_s)

    # Phase 2 — Pre-process (burst-pool gated)
    burst_wait_start = env.now
    yield from burst_pool.acquire(p.preprocess_peak_ram_gb)
    burst_wait_s = env.now - burst_wait_start

    metrics.phase_transition(env.now, jid, PhaseID.PREPROCESS, node.node_id)
    node.update_phase(jid, PhaseID.PREPROCESS,
                      ram_gb=p.preprocess_peak_ram_gb, vcpu=p.preprocess_vcpu)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.preprocess_duration_s)
    burst_pool.release(p.preprocess_peak_ram_gb)

    # Phase 3 — Workhorse
    node.update_phase(jid, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb, vcpu=0.0)
    metrics.phase_transition(env.now, jid, PhaseID.WORKHORSE, node.node_id)
    for stage in p.stages:
        node.update_phase(jid, PhaseID.WORKHORSE,
                          ram_gb=p.workhorse_ram_gb, vcpu=stage.effective_threads)
        scheduler.cpu_boost(env, node, metrics)
        yield env.timeout(stage.wall_clock_seconds)

    # Phase 4 — Upload
    metrics.phase_transition(env.now, jid, PhaseID.UPLOAD, node.node_id)
    node.update_phase(jid, PhaseID.UPLOAD, ram_gb=p.upload_ram_gb, vcpu=1.0)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.upload_duration_s)

    node.remove_job(jid)
    metrics.job_complete(
        t=env.now, job_id=jid, centroid_id=job.centroid_id,
        node_id=node.node_id, queue_wait_s=queue_wait_s,
        total_elapsed_s=env.now - arrival_time,
        retry_count=job.retry_count,
    )
    metrics.record(SimEvent(EventType.COST_SAMPLE, env.now, {
        'type': 'burst_wait', 'job_id': jid,
        'pool': scheduler._job_pool_id.get(jid, '?'),
        'node_id': node.node_id, 'wait_s': round(burst_wait_s, 2),
    }))
    scheduler.on_job_complete(env, node, job)


# ---------------------------------------------------------------------------
# Multi-pool K8S+ scheduler
# ---------------------------------------------------------------------------

class K8SPlusScheduler:
    """
    K8S+ scheduler with admin-configured RAM-band pools.

    Each pool covers a RAM band (exclusive_min_gb, inclusive_max_gb] and maps
    to a specific instance class. The scheduled_zone per node is:
        node_ram_gb - k8s_os_overhead_gb - pool.daemonset_headroom_gb
    The daemonset_headroom_gb doubles as the NodeBurstPool headroom for Phase 2.

    Age cordoning (BSIM-90): a SimPy timer fires after pool.age_cordon_s and
    marks the node cordoned — no new placements — while in-flight jobs finish.
    """

    def __init__(
        self,
        cfg: K8SPlusSchedulerConfig,
        registry: InstanceRegistry,
        metrics: MetricsCollector,
        centroid_peak_rams: list[float] | None = None,  # unused; kept for call-site compat
        rng: Optional[random.Random] = None,
    ) -> None:
        self.cfg = cfg
        self.registry = registry
        self.metrics = metrics
        self.rng = rng or random.Random(42)

        self._instance_by_name = {t.name: t for t in registry.all_types}
        self._sorted_pools = sorted(cfg.pools, key=lambda p: p.exclusive_min_gb)

        # Per-node state
        self._nodes: dict[str, NodeModel] = {}
        self._burst_pools: dict[str, NodeBurstPool] = {}
        self._accruers: dict[str, NodeCostAccruer] = {}
        self._reserved: dict[str, str] = {}      # node_id → job_id (panic reservation)
        self._node_pool: dict[str, K8SPlusPoolConfig] = {}  # node_id → pool config
        self._cordoned: set[str] = set()          # BSIM-90: node_ids barred from new placements

        # Per-job state
        self._queue = JobQueue()
        self._panic_monitors: dict[str, simpy.Process] = {}
        self._job_pool_id: dict[str, str] = {}   # job_id → pool.id for metrics

        self._env: Optional[simpy.Environment] = None

    # ------------------------------------------------------------------
    # Pool routing
    # ------------------------------------------------------------------

    def _route_job(self, peak_ram_gb: float) -> Optional[K8SPlusPoolConfig]:
        for pool in self._sorted_pools:
            if pool.exclusive_min_gb < peak_ram_gb <= pool.inclusive_max_gb:
                return pool
        return None

    def _scheduled_zone(self, pool: K8SPlusPoolConfig) -> float:
        instance = self._instance_by_name.get(pool.instance_class)
        if instance is None:
            return 0.0
        return max(0.0, instance.ram_gb - self.cfg.k8s_os_overhead_gb - pool.daemonset_headroom_gb)

    # ------------------------------------------------------------------
    # Placement
    # ------------------------------------------------------------------

    def _fits(self, node: NodeModel, pool: K8SPlusPoolConfig, soft_gb: float, vcpu: int) -> bool:
        zone = self._scheduled_zone(pool)
        return (
            zone > 0
            and node.allocated_ram_gb + soft_gb <= zone
            and node.allocated_vcpu + vcpu <= node.physical_vcpu
        )

    def _best_fit_node(
        self, pool: K8SPlusPoolConfig, soft_gb: float, vcpu: int, job_id: str
    ) -> Optional[NodeModel]:
        candidates = [
            (node.allocated_ram_gb, node)
            for node_id, node in self._nodes.items()
            if node.state == NodeStateEnum.READY
            and node_id not in self._cordoned
            and self._node_pool.get(node_id) is pool
            and self._reserved.get(node_id, job_id) == job_id
            and self._fits(node, pool, soft_gb, vcpu)
        ]
        return max(candidates, key=lambda x: x[0])[1] if candidates else None

    def _try_schedule(self, env: simpy.Environment) -> None:
        changed = True
        while changed and self._queue:
            changed = False
            for entry in sorted(self._queue._heap):
                if self._place_job(env, entry):
                    self._queue._heap = [
                        e for e in self._queue._heap
                        if e.job.job_id != entry.job.job_id
                    ]
                    heapq.heapify(self._queue._heap)
                    changed = True
                    break

    def _place_job(self, env: simpy.Environment, entry) -> bool:
        job = entry.job
        p = job.profile
        pool = self._route_job(p.preprocess_peak_ram_gb)
        if pool is None:
            return False
        soft = p.soft_limit_ram_gb
        vcpu = p.workhorse_declared_vcpu
        best = self._best_fit_node(pool, soft, vcpu, job.job_id)
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

        self._job_pool_id[job.job_id] = pool.id
        bp = self._burst_pools[best.node_id]
        env.process(_run_job(
            env=env, job=job, node=best, metrics=self.metrics,
            burst_pool=bp,
            arrival_time=entry.arrival_time,
            queue_entry_time=entry.enqueue_time,
            scheduler=self,
        ))
        return True

    # ------------------------------------------------------------------
    # Public scheduler interface
    # ------------------------------------------------------------------

    def on_job_arrival(self, env: simpy.Environment, job: JobSpec, arrival_time: float) -> None:
        if self._env is None:
            self._env = env

        pool = self._route_job(job.profile.preprocess_peak_ram_gb)
        if pool is None:
            self.metrics.job_terminal(env.now, job.job_id, job.centroid_id)
            return

        self.metrics.job_queued(env.now, job.job_id, job.centroid_id, "NORMAL")
        self._queue.enqueue(job, arrival_time=arrival_time,
                            priority=Priority.NORMAL, enqueue_time=env.now)
        proc = env.process(self._panic_monitor(env, job, enqueue_time=env.now))
        self._panic_monitors[job.job_id] = proc
        self._try_schedule(env)

    def on_job_complete(self, env: simpy.Environment, node: NodeModel, job: JobSpec) -> None:
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

    def guarantee_capacity(self, env: simpy.Environment, job: JobSpec) -> None:
        """Panic-path: reserve an existing node or spawn a new one for this job."""
        pool = self._route_job(job.profile.preprocess_peak_ram_gb)
        if pool is None:
            return
        soft = job.profile.soft_limit_ram_gb
        vcpu = job.profile.workhorse_declared_vcpu
        for node_id, node in self._nodes.items():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node_id not in self._cordoned
                    and node_id not in self._reserved
                    and self._node_pool.get(node_id) is pool
                    and self._fits(node, pool, soft, vcpu)):
                self._reserved[node_id] = job.job_id
                return
        instance = self._instance_by_name.get(pool.instance_class)
        if instance:
            env.process(self._launch_node(env, instance, pool, for_job=job))

    # ------------------------------------------------------------------
    # Node lifecycle
    # ------------------------------------------------------------------

    def _launch_node(
        self,
        env: simpy.Environment,
        instance,
        pool: K8SPlusPoolConfig,
        for_job: Optional[JobSpec] = None,
    ):
        node_id = str(uuid.uuid4())[:8]
        self.metrics.node_launching(env.now, node_id, instance.name)
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.cfg.k8s_os_overhead_gb)
        self._nodes[node_id] = node
        self._node_pool[node_id] = pool

        # NodeBurstPool headroom fixed at admin-configured daemonset_headroom_gb (BSIM-89)
        bp = NodeBurstPool(env=env, node_physical_ram_gb=instance.ram_gb,
                           os_overhead_gb=self.cfg.k8s_os_overhead_gb)
        bp.update_max_peak(pool.daemonset_headroom_gb)
        self._burst_pools[node_id] = bp

        self._accruers[node_id] = NodeCostAccruer(
            node_id=node_id, instance=instance, launch_time=env.now)

        # BSIM-90: age-based cordoning timer
        env.process(self._age_cordon_timer(env, node_id, pool.age_cordon_s))

        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if for_job:
            self._reserved[node_id] = for_job.job_id
        self._try_schedule(env)

    def _age_cordon_timer(self, env: simpy.Environment, node_id: str, age_s: float):
        """BSIM-90: After age_s, mark node cordoned (no new placements)."""
        yield env.timeout(age_s)
        if node_id in self._nodes and self._nodes[node_id].state != NodeStateEnum.TERMINATED:
            self._cordoned.add(node_id)

    def _panic_monitor(self, env: simpy.Environment, job: JobSpec, enqueue_time: float):
        try:
            yield env.timeout(self.cfg.panic_threshold_seconds)
        except simpy.Interrupt:
            return
        self.metrics.panic_trigger(env.now, job.job_id, env.now - enqueue_time)
        self._queue.elevate_to_urgent(job.job_id)
        self.guarantee_capacity(env, job)

    def _idle_timer(self, env: simpy.Environment, node: NodeModel):
        idle_start = env.now
        yield env.timeout(self.cfg.idle_timeout_seconds)
        if node.state == NodeStateEnum.IDLE and node.job_count == 0:
            node.state = NodeStateEnum.TERMINATED
            self._cordoned.discard(node.node_id)
            self.metrics.node_terminated(env.now, node.node_id, env.now - idle_start)
            accruer = self._accruers.get(node.node_id)
            if accruer:
                accruer.terminate(env.now)

    # ------------------------------------------------------------------
    # CPU boost (BSIM-71 wiring)
    # ------------------------------------------------------------------

    def cpu_boost(self, env, node, metrics):
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_k8s
        run_cpu_boost_k8s(env, node, metrics)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    @property
    def accruers(self) -> list[NodeCostAccruer]:
        return list(self._accruers.values())

    def finalize(self, env: simpy.Environment) -> None:
        for accruer in self._accruers.values():
            if not accruer.is_terminated:
                accruer.terminate(env.now)

    def capacity_report(self) -> dict:
        report = {}
        for pool in self.cfg.pools:
            instance = self._instance_by_name.get(pool.instance_class)
            if instance is None:
                continue
            zone = self._scheduled_zone(pool)
            report[pool.id] = {
                'instance_class': pool.instance_class,
                'instance_ram_gb': instance.ram_gb,
                'scheduled_zone_gb': round(zone, 1),
                'daemonset_headroom_gb': pool.daemonset_headroom_gb,
                'band': f"({pool.exclusive_min_gb}, {pool.inclusive_max_gb}]",
                'age_cordon_s': pool.age_cordon_s,
                'cordoned_nodes': sum(
                    1 for nid in self._cordoned
                    if self._node_pool.get(nid) is pool
                ),
            }
        return report

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
