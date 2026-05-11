"""BSIM-25 through 30: K8S / OKD Scheduler."""
from __future__ import annotations
import uuid, random
from typing import Optional
import simpy
from batch_sim.core.engine import NodeModel, JobQueue, Priority, QueueEntry, OverloadHandler, run_job_process
from batch_sim.core.schemas import SchedulerConfig
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import MetricsCollector, NodeState as NodeStateEnum
from batch_sim.registry.instance_registry import InstanceRegistry, NodeCostAccruer, compute_k8s_capacity, K8SCapacityProfile
from batch_sim.core.schemas import InstanceTypeConfig


class K8SScheduler:
    def __init__(self, cfg, registry, metrics, centroid_peak_rams, rng=None):
        self.cfg = cfg; self.registry = registry; self.metrics = metrics
        self.centroid_peak_rams = centroid_peak_rams
        self.rng = rng or random.Random(42)
        self._queue = JobQueue(); self._nodes = {}; self._accruers = {}
        self._reserved = {}; self._capacity_cache = {}
        self._overload_handler = None; self._panic_monitors = {}

    def _setup(self, env):
        self._env = env
        self._overload_handler = OverloadHandler(
            metrics=self.metrics, scheduler_cfg=self.cfg,
            replay_queue=self._queue, rng=self.rng)

    def _k8s_capacity(self, instance):
        if instance.name not in self._capacity_cache:
            self._capacity_cache[instance.name] = compute_k8s_capacity(
                instance=instance, centroid_peak_rams=self.centroid_peak_rams,
                os_overhead_gb=self.cfg.k8s_os_overhead_gb)
        return self._capacity_cache[instance.name]

    def on_job_arrival(self, env, job, arrival_time):
        if not hasattr(self, "_env"): self._setup(env)
        self.metrics.job_queued(env.now, job.job_id, job.centroid_id, "NORMAL")
        self._queue.enqueue(job, arrival_time=arrival_time, priority=Priority.NORMAL, enqueue_time=env.now)
        proc = env.process(self._panic_monitor(env, job, enqueue_time=env.now))
        self._panic_monitors[job.job_id] = proc
        self._try_schedule(env)

    def on_job_complete(self, env, node, job):
        soft = job.profile.soft_limit_ram_gb
        node.allocated_ram_gb = max(0.0, node.allocated_ram_gb - soft)
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - job.profile.workhorse_declared_vcpu)
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        if node.job_count == 0:
            node.state = NodeStateEnum.IDLE; node.idle_since = env.now
            self.metrics.node_idle(env.now, node.node_id)
            env.process(self._idle_timer(env, node))
        self._try_schedule(env)

    def guarantee_capacity(self, env, job):
        soft = job.profile.soft_limit_ram_gb; vcpu = job.profile.workhorse_declared_vcpu
        for node in self._nodes.values():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node.node_id not in self._reserved
                    and self._k8s_fits(node, soft, vcpu)):
                self._reserved[node.node_id] = job.job_id; return
        instance = self.registry.cheapest_fitting(
            min_ram_gb=job.profile.peak_ram_gb + self.cfg.k8s_os_overhead_gb, min_vcpu=vcpu)
        if instance: env.process(self._launch_node(env, instance, for_job=job))

    def _try_schedule(self, env):
        """Scan full queue for placeable jobs; do not stop at first unplaceable
        entry (avoids deadlock when a warming node is reserved for a later job
        while an already-ready node could serve an earlier-queued job)."""
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
        job = entry.job; p = job.profile
        soft = p.soft_limit_ram_gb; vcpu = p.workhorse_declared_vcpu
        best = self._best_fit_node(soft, vcpu, job.job_id)
        if best is None: return False
        mon = self._panic_monitors.pop(job.job_id, None)
        if mon and mon.is_alive: mon.interrupt("placed")
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += soft; best.allocated_vcpu += vcpu
        if best.state == NodeStateEnum.IDLE: best.state = NodeStateEnum.READY
        env.process(run_job_process(env=env, job=job, node=best, metrics=self.metrics,
            overload_handler=self._overload_handler, arrival_time=entry.arrival_time,
            queue_entry_time=entry.enqueue_time, scheduler=self))
        return True

    def _k8s_fits(self, node, soft_gb, vcpu):
        cap = self._capacity_cache.get(node.instance.name)
        if cap is None or cap.effective_schedulable_gb <= 0: return False
        return (node.allocated_ram_gb + soft_gb <= cap.effective_schedulable_gb
                and node.allocated_vcpu + vcpu <= node.physical_vcpu)

    def _best_fit_node(self, soft_gb, vcpu, job_id):
        candidates = [(node.allocated_ram_gb, node)
            for node in self._nodes.values()
            if node.state == NodeStateEnum.READY
            and self._reserved.get(node.node_id, job_id) == job_id
            and self._k8s_fits(node, soft_gb, vcpu)]
        if not candidates: return None
        return max(candidates, key=lambda x: x[0])[1]

    def _launch_node(self, env, instance, for_job=None):
        node_id = str(uuid.uuid4())[:8]
        self.metrics.node_launching(env.now, node_id, instance.name)
        cap = self._k8s_capacity(instance)
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.cfg.k8s_os_overhead_gb)
        self._nodes[node_id] = node
        self._capacity_cache[instance.name] = cap
        self._accruers[node_id] = NodeCostAccruer(node_id=node_id, instance=instance, launch_time=env.now)
        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if for_job: self._reserved[node_id] = for_job.job_id
        self._try_schedule(env)

    def _panic_monitor(self, env, job, enqueue_time):
        try: yield env.timeout(self.cfg.panic_threshold_seconds)
        except simpy.Interrupt: return
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
            if accruer: accruer.terminate(env.now)

    @property
    def accruers(self): return list(self._accruers.values())

    def finalize(self, env):
        for accruer in self._accruers.values():
            if not accruer.is_terminated: accruer.terminate(env.now)

    def capacity_report(self):
        return {name: {"tier_local_mm_gb": cap.tier_local_mm_gb,
            "effective_schedulable_gb": cap.effective_schedulable_gb,
            "soft_limit_gb": cap.soft_limit_gb,
            "max_schedulable_jobs": cap.max_schedulable_jobs,
            "headroom_pct": round(cap.headroom_pct, 1)}
            for name, cap in self._capacity_cache.items()}
