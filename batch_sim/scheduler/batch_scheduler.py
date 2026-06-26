"""BSIM-20 through 24: AWS Batch Scheduler."""
from __future__ import annotations
import uuid, random
from typing import Optional
import simpy
from batch_sim.core.engine import NodeModel, JobQueue, QueueEntry, OverloadHandler, run_job_process
from batch_sim.core.schemas import SchedulerConfig
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import MetricsCollector, NodeState as NodeStateEnum
from batch_sim.registry.instance_registry import InstanceRegistry, NodeCostAccruer, workspace_gb
from batch_sim.scheduler.storage_pool import NodeStoragePool


class BatchScheduler:
    def __init__(self, cfg, registry, metrics, rng=None, os_overhead_gb=0.0):
        self.cfg = cfg; self.registry = registry; self.metrics = metrics
        self.rng = rng or random.Random(42); self.os_overhead_gb = os_overhead_gb
        self._queue = JobQueue(); self._nodes = {}; self._accruers = {}
        self._storage_pools: dict[str, NodeStoragePool] = {}
        self._reserved = {}; self._overload_handler = None

    def _setup(self, env):
        self._env = env
        self._overload_handler = OverloadHandler(
            metrics=self.metrics, scheduler_cfg=self.cfg,
            replay_queue=self._queue, rng=self.rng)
        env.process(self._scale_out_monitor(env))

    def on_job_arrival(self, env, job, arrival_time):
        if not hasattr(self, "_env"): self._setup(env)
        self.metrics.job_queued(env.now, job.job_id, job.centroid_id, "NORMAL")
        self._queue.enqueue(job, arrival_time=arrival_time, enqueue_time=env.now)
        self._try_schedule(env)

    def on_job_complete(self, env, node, job):
        p = job.profile
        node.allocated_ram_gb = max(0.0, node.allocated_ram_gb - p.peak_ram_gb)
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - (getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu))
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        pool = self._storage_pools.get(node.node_id)
        if pool is not None:
            pool.job_exit(workspace_gb(job))
        if node.job_count == 0:
            node.state = NodeStateEnum.IDLE; node.idle_since = env.now
            self.metrics.node_idle(env.now, node.node_id)
            env.process(self._idle_timer(env, node))
        self._try_schedule(env)

    def _allowed_types(self) -> list:
        """BSIM-115: scope the registry to cfg.allowed_instance_types when set."""
        allowed = self.cfg.allowed_instance_types
        if allowed is None:
            return self.registry.all_types
        allowed_set = set(allowed)
        return [t for t in self.registry.all_types if t.name in allowed_set]

    def _cheapest_fitting(self, min_ram_gb: float, min_vcpu: int):
        for t in self._allowed_types():
            if t.ram_gb >= min_ram_gb and t.vcpu >= min_vcpu:
                return t
        return None

    def _scale_out_monitor(self, env):
        """
        Polling coroutine: when the oldest queued job has waited at least
        cfg.scale_out_threshold_s, calls _provision_to_demand to launch nodes
        for the entire queue at once, then sleeps cfg.scale_out_poll_s.
        """
        while True:
            if not self._queue:
                yield env.timeout(self.cfg.scale_out_poll_s)
                continue

            oldest_wait = max(env.now - e.enqueue_time for e in self._queue._heap)
            remaining = self.cfg.scale_out_threshold_s - oldest_wait

            if remaining > 1e-6:
                yield env.timeout(remaining)
            else:
                self._provision_to_demand(env)
                yield env.timeout(self.cfg.scale_out_poll_s)

    def _provision_to_demand(self, env):
        """
        Karpenter-style demand provisioning: score every instance type against
        the full overflow queue and launch whichever type packs the most unserved
        jobs per dollar, repeating until all overflow is covered or no suitable
        type exists.  Models AWS Batch demand-based autoscaling with queue-aware
        node sizing rather than cheapest-per-job selection.
        """
        virtual = [
            [n.physical_ram_gb - n.allocated_ram_gb,
             n.physical_vcpu - n.allocated_vcpu]
            for n in self._nodes.values()
            if n.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
        ]
        overflow = []
        for entry in sorted(self._queue._heap):
            job = entry.job; p = job.profile
            _vcpu = getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu
            ram = p.peak_ram_gb
            for vn in virtual:
                if vn[0] >= ram and vn[1] >= _vcpu:
                    vn[0] -= ram; vn[1] -= _vcpu
                    break
            else:
                overflow.append((ram, _vcpu))

        while overflow:
            inst = self._select_instance_for_overflow(overflow)
            if inst is None:
                break
            env.process(self._launch_node(env, inst))
            rem_ram, rem_vcpu = inst.ram_gb, inst.vcpu
            remaining = []
            for ram, vcpu in overflow:
                if rem_ram >= ram and rem_vcpu >= vcpu:
                    rem_ram -= ram; rem_vcpu -= vcpu
                else:
                    remaining.append((ram, vcpu))
            overflow = remaining

    def _select_instance_for_overflow(self, overflow: list) -> Optional[Any]:
        """Score each instance type by (jobs fitting greedily / hourly rate).
        Ties broken by raw job count so equal-rate types resolve toward larger
        instances, reducing total node count and warmup overhead."""
        best_inst, best_score = None, (-1.0, -1)
        for inst in self._allowed_types():
            rem_ram, rem_vcpu = inst.ram_gb, inst.vcpu
            count = 0
            for ram, vcpu in sorted(overflow, reverse=True):
                if rem_ram >= ram and rem_vcpu >= vcpu:
                    rem_ram -= ram; rem_vcpu -= vcpu
                    count += 1
            if count > 0:
                # Primary: most jobs packed (minimises node count and warmup overhead).
                # Secondary: highest jobs/dollar when counts are equal.
                score = (count, count / inst.hourly_price_usd)
                if score > best_score:
                    best_score = score
                    best_inst = inst
        return best_inst

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
        _vcpu = getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu
        best = self._best_fit_node(p.peak_ram_gb, _vcpu, job.job_id)
        if best is None: return False
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += p.peak_ram_gb; best.allocated_vcpu += _vcpu
        if best.state == NodeStateEnum.IDLE: best.state = NodeStateEnum.READY
        pool = self._storage_pools.get(best.node_id)
        if pool is not None:
            pool.job_start(env.now, job.job_id, workspace_gb(job), self.metrics)
        env.process(run_job_process(env=env, job=job, node=best, metrics=self.metrics,
            overload_handler=self._overload_handler, arrival_time=entry.arrival_time,
            queue_entry_time=entry.enqueue_time, scheduler=self))
        return True

    def _batch_fits(self, node, ram_gb, vcpu):
        return (node.allocated_ram_gb + ram_gb <= node.physical_ram_gb
                and node.allocated_vcpu + vcpu <= node.physical_vcpu)

    def _best_fit_node(self, ram_gb, vcpu, job_id):
        candidates = [(node.allocated_ram_gb + node.allocated_vcpu, node)
            for node in self._nodes.values()
            if node.state == NodeStateEnum.READY
            and self._reserved.get(node.node_id, job_id) == job_id
            and self._batch_fits(node, ram_gb, vcpu)]
        if not candidates: return None
        return max(candidates, key=lambda x: x[0])[1]

    def _launch_node(self, env, instance, for_job=None):
        node_id = str(uuid.uuid4())[:8]
        self.metrics.node_launching(env.now, node_id, instance.name)
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.os_overhead_gb)
        self._nodes[node_id] = node
        self._accruers[node_id] = NodeCostAccruer(node_id=node_id, instance=instance, launch_time=env.now)
        if self.cfg.storage is not None:
            self._storage_pools[node_id] = NodeStoragePool(
                node_id=node_id, config=self.cfg.storage,
                instance=instance, open_time=env.now)
        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if for_job: self._reserved[node_id] = for_job.job_id
        self._try_schedule(env)

    def _idle_timer(self, env, node):
        idle_start = env.now
        yield env.timeout(self.cfg.idle_timeout_seconds)
        if node.state == NodeStateEnum.IDLE and node.job_count == 0:
            node.state = NodeStateEnum.TERMINATED
            self.metrics.node_terminated(env.now, node.node_id, env.now - idle_start)
            accruer = self._accruers.get(node.node_id)
            if accruer: accruer.terminate(env.now)
            pool = self._storage_pools.get(node.node_id)
            if pool is not None: pool.close(env.now)

    def cpu_boost(self, env, node, metrics):
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_batch
        run_cpu_boost_batch(env, node, metrics)

    @property
    def accruers(self): return list(self._accruers.values())

    @property
    def storage_pools(self) -> list[NodeStoragePool]:
        return list(self._storage_pools.values())

    def finalize(self, env):
        for node_id, accruer in self._accruers.items():
            if not accruer.is_terminated:
                accruer.terminate(env.now)
                node = self._nodes.get(node_id)
                idle_since = (node.idle_since if node and node.idle_since >= 0
                              else env.now)
                self.metrics.node_terminated(env.now, node_id, env.now - idle_since)
                if node:
                    node.state = NodeStateEnum.TERMINATED
        for pool in self._storage_pools.values():
            pool.close(env.now)
