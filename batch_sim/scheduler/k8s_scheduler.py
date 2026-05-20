"""BSIM-25 through 30: K8S / OKD Scheduler.

BSIM-83/84/85: Extended with optional time-window scheduling policy.
When cfg.time_window_policy is set the scheduler:
  - routes each job to the QueuePolicy whose RAM band matches
  - spawns the queue's spawn_instance_class rather than cheapest_fitting
  - replaces the fixed idle_timeout with per-queue drain rules
  - swaps the active window at each 24-hour boundary (POLICY_SWAP event)
  - transitions nodes to DRAINING when a drain rule fires (NODE_DRAINING event)

When time_window_policy is absent, all pre-E16 behaviour is preserved.
"""
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

        # BSIM-84/85: policy state
        self._active_window_idx: int = 0
        self._drain_monitors: dict[str, simpy.Process] = {}
        self._node_queue_policy: dict[str, object] = {}  # node_id → QueuePolicy
        self._last_spawn_t: dict[str, float] = {}        # queue band key → last spawn time

    def _setup(self, env):
        self._env = env
        self._overload_handler = OverloadHandler(
            metrics=self.metrics, scheduler_cfg=self.cfg,
            replay_queue=self._queue, rng=self.rng)
        if self.cfg.time_window_policy:
            windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
            self._active_window_idx = self._find_window_idx(env.now % 86400.0, windows)
            env.process(self._policy_timer(env, windows))

    # -----------------------------------------------------------------------
    # BSIM-84: time-window helpers
    # -----------------------------------------------------------------------

    def _find_window_idx(self, tod_s: float, windows: list) -> int:
        """Return the index of the window that covers time-of-day tod_s."""
        for i, w in enumerate(windows):
            if w.start_time_s <= tod_s < w.end_time_s:
                return i
        return len(windows) - 1  # last window covers up to 86400

    def _active_window(self) -> object | None:
        """Return the currently active TimeWindowPolicy, or None."""
        if not self.cfg.time_window_policy:
            return None
        windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
        return windows[self._active_window_idx]

    def _route_to_queue(self, peak_ram_gb: float) -> object | None:
        """Find the QueuePolicy for a job's peak RAM in the active window."""
        window = self._active_window()
        if window is None:
            return None
        for q in window.queues:
            if q.exclusive_min_gb < peak_ram_gb <= q.inclusive_max_gb:
                return q
        return None  # no band covers this RAM — job rejected at placement

    def _policy_timer(self, env, windows: list):
        """Process: wakes at each window boundary and swaps the active policy."""
        while True:
            current_idx = self._active_window_idx
            current_window = windows[current_idx]
            next_boundary_tod = current_window.end_time_s

            # Time until next boundary in sim time
            current_tod = env.now % 86400.0
            if next_boundary_tod > current_tod:
                delay = next_boundary_tod - current_tod
            else:
                # Boundary already passed today — wait until tomorrow
                delay = 86400.0 - current_tod + next_boundary_tod

            yield env.timeout(delay)

            new_idx = (current_idx + 1) % len(windows)
            old_w = windows[current_idx]
            new_w = windows[new_idx]
            self.metrics.policy_swap(env.now, old_w.start_time_s, new_w.start_time_s)
            self._active_window_idx = new_idx

            # Interrupt all drain monitors so they re-evaluate with new rules
            for proc in list(self._drain_monitors.values()):
                if proc.is_alive:
                    proc.interrupt("policy_swap")

            self._try_schedule(env)

    # -----------------------------------------------------------------------
    # BSIM-85: drain rule helpers
    # -----------------------------------------------------------------------

    def _best_drain_rule(self, node_id: str, idle_vcpu: float) -> object | None:
        """Return the most aggressive drain rule that applies (highest threshold ≤ idle_vcpu)."""
        qp = self._node_queue_policy.get(node_id)
        if qp is None or not qp.drain_rules:
            return None
        applicable = [r for r in qp.drain_rules if idle_vcpu >= r.idle_vcpu]
        if not applicable:
            return None
        return max(applicable, key=lambda r: r.idle_vcpu)

    def _drain_monitor(self, env, node):
        """
        Per-node process: evaluates drain rules continuously.
        Interrupted whenever a job is placed or completes on this node, or
        when the active policy swaps.
        """
        while node.state not in (NodeStateEnum.TERMINATED, NodeStateEnum.DRAINING):
            idle_vcpu = node.physical_vcpu - node.allocated_vcpu
            rule = self._best_drain_rule(node.node_id, idle_vcpu)

            if rule is None:
                # No rule applies — park until something changes
                try:
                    yield env.timeout(float('inf'))
                except simpy.Interrupt:
                    continue

            try:
                yield env.timeout(rule.duration_s)
            except simpy.Interrupt:
                continue

            # Timer fired — check state is still valid before draining
            if node.state in (NodeStateEnum.TERMINATED, NodeStateEnum.DRAINING):
                return
            node.state = NodeStateEnum.DRAINING
            self.metrics.node_draining(env.now, node.node_id, rule.idle_vcpu)
            return

    def _interrupt_drain_monitor(self, node_id: str) -> None:
        proc = self._drain_monitors.get(node_id)
        if proc and proc.is_alive:
            proc.interrupt("state_change")

    # -----------------------------------------------------------------------
    # Core capacity helpers (unchanged)
    # -----------------------------------------------------------------------

    def _k8s_capacity(self, instance):
        if instance.name not in self._capacity_cache:
            self._capacity_cache[instance.name] = compute_k8s_capacity(
                instance=instance, centroid_peak_rams=self.centroid_peak_rams,
                os_overhead_gb=self.cfg.k8s_os_overhead_gb)
        return self._capacity_cache[instance.name]

    # -----------------------------------------------------------------------
    # Job lifecycle
    # -----------------------------------------------------------------------

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
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - (getattr(job, "soft_cpu", 0) or job.profile.workhorse_declared_vcpu))
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}

        if node.job_count == 0:
            if node.state == NodeStateEnum.DRAINING:
                # BSIM-85: last job gone from a draining node — terminate immediately
                node.state = NodeStateEnum.TERMINATED
                self.metrics.node_terminated(env.now, node.node_id, 0)
                accruer = self._accruers.get(node.node_id)
                if accruer:
                    accruer.terminate(env.now)
            else:
                node.state = NodeStateEnum.IDLE; node.idle_since = env.now
                self.metrics.node_idle(env.now, node.node_id)
                env.process(self._idle_timer(env, node))

        # Interrupt drain monitor to re-evaluate with updated idle_vcpu
        self._interrupt_drain_monitor(node.node_id)
        self._try_schedule(env)

    def guarantee_capacity(self, env, job):
        soft = job.profile.soft_limit_ram_gb
        vcpu = (getattr(job, "soft_cpu", 0) or job.profile.workhorse_declared_vcpu)
        for node in self._nodes.values():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node.node_id not in self._reserved
                    and self._k8s_fits(node, soft, vcpu)):
                self._reserved[node.node_id] = job.job_id; return

        instance = self._select_instance_for_job(job)
        if instance:
            env.process(self._launch_node(env, instance, for_job=job))

    def _select_instance_for_job(self, job):
        """
        Pick the instance to launch for this job.
        With time_window_policy: use the queue's spawn_instance_class.
        Without: cheapest_fitting (legacy behaviour).
        """
        if self.cfg.time_window_policy:
            qp = self._route_to_queue(job.profile.preprocess_peak_ram_gb)
            if qp is None:
                return None
            # Spawn rate cooldown: at most one node per (60 / spawn_rate_per_min) seconds
            band_key = f"{qp.exclusive_min_gb}-{qp.inclusive_max_gb}"
            now = getattr(self, '_env', None)
            if now is not None:
                now = self._env.now
                cooldown = 60.0 / qp.spawn_rate_per_min
                last = self._last_spawn_t.get(band_key, -cooldown)
                if now - last < cooldown:
                    return None   # rate-limited: caller will retry on next placement
                self._last_spawn_t[band_key] = now
            instance = self.registry.get_by_name(qp.spawn_instance_class)
            return instance
        else:
            vcpu = (getattr(job, "soft_cpu", 0) or job.profile.workhorse_declared_vcpu)
            return self.registry.cheapest_fitting(
                min_ram_gb=job.profile.peak_ram_gb + self.cfg.k8s_os_overhead_gb,
                min_vcpu=vcpu)

    def _try_schedule(self, env):
        """Scan full queue for placeable jobs."""
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
        soft = p.soft_limit_ram_gb; vcpu = (getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu)
        best = self._best_fit_node(soft, vcpu, job.job_id)
        if best is None: return False
        mon = self._panic_monitors.pop(job.job_id, None)
        if mon and mon.is_alive: mon.interrupt("placed")
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += soft; best.allocated_vcpu += vcpu
        if best.state == NodeStateEnum.IDLE: best.state = NodeStateEnum.READY
        # Interrupt drain monitor: node got busier
        self._interrupt_drain_monitor(best.node_id)
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
            if node.state == NodeStateEnum.READY           # DRAINING excluded
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

        # BSIM-84: record which queue policy launched this node
        if self.cfg.time_window_policy and for_job is not None:
            qp = self._route_to_queue(for_job.profile.preprocess_peak_ram_gb)
            if qp is not None:
                self._node_queue_policy[node_id] = qp

        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if for_job: self._reserved[node_id] = for_job.job_id

        # BSIM-85: start drain monitor when policy is active
        if self.cfg.time_window_policy:
            proc = env.process(self._drain_monitor(env, node))
            self._drain_monitors[node_id] = proc

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

    def cpu_boost(self, env, node, metrics):
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_k8s
        run_cpu_boost_k8s(env, node, metrics)

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
