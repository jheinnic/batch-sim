"""BSIM-25 through 30: K8S / OKD Scheduler.

BSIM-83/84/85: Extended with optional time-window scheduling policy.
BSIM-104-108: Tier-compatibility model — tiers carry spike_max_gb as a hardware
constant; centroids declare the set of tiers their jobs may run on; nodes are
tagged to one tier at launch; placement uses set membership; provisioning is
solved jointly across tiers that share an instance type.

Tier mode (cfg.tiers non-empty):
  - job resolved to a compatible-tier SET at arrival, via centroid.compatible_tiers
    with optional per-centroid time-window override (BSIM-105/106)
  - K8S capacity uses tier.spike_max_gb, not job-spec data (BSIM-102/104)
  - placement: a node is a candidate when its tier is in the job's compatible set
    (BSIM-106)
  - provisioning: jobs grouped by spawn_instance_class; the tier configuration that
    packs the most eligible jobs per node is launched (BSIM-107)
  - admission: a job whose burst exceeds every compatible tier's spike is rejected;
    declared tiers that cannot host the burst are warned and dropped (BSIM-108)

Legacy mode (cfg.tiers empty):
  - RAM-band routing via time_window_policy QueuePolicy.exclusive_min_gb /
    inclusive_max_gb; or cheapest_fitting when no policy is set
  - centroid_peak_rams drives spike headroom as before
"""
from __future__ import annotations
import uuid, random
from typing import Any, Generator, Optional
import simpy
from batch_sim.core.engine import NodeModel, JobQueue, Priority, QueueEntry, OverloadHandler, run_job_process
from batch_sim.core.schemas import (
    SchedulerConfig, DrainRule, QueuePolicy, TimeWindowPolicy, TierProfile,
)
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import (
    MetricsCollector, NodeState as NodeStateEnum, EventType, SimEvent,
)
from batch_sim.registry.instance_registry import (
    InstanceRegistry, NodeCostAccruer, compute_k8s_capacity, K8SCapacityProfile, workspace_gb,
)
from batch_sim.core.schemas import InstanceTypeConfig
from batch_sim.scheduler.storage_pool import GenerationalStoragePool


class K8SScheduler:
    def __init__(self, cfg: Any, registry: InstanceRegistry, metrics: MetricsCollector,
                 centroid_peak_rams: list[float],
                 centroid_tier_config: "dict[str, dict] | None" = None,
                 rng: Any = None) -> None:
        self.cfg = cfg; self.registry = registry; self.metrics = metrics
        self.centroid_peak_rams = centroid_peak_rams
        self.rng = rng or random.Random(42)
        self._queue = JobQueue(); self._nodes = {}; self._accruers = {}
        self._storage_pools: dict[str, GenerationalStoragePool] = {}
        self._reserved = {}
        # Cache keyed by (instance_name, tier_name_or_empty_string)
        self._capacity_cache: dict[tuple[str, str], K8SCapacityProfile] = {}
        self._overload_handler = OverloadHandler(
            metrics=self.metrics, scheduler_cfg=self.cfg,
            replay_queue=self._queue, rng=self.rng)
        self._panic_monitors: dict[str, simpy.Process] = {}

        # BSIM-84/85: time-window policy state
        self._active_window_idx: int = 0
        self._drain_monitors: dict[str, simpy.Process] = {}
        self._node_queue_policy: dict[str, QueuePolicy] = {}  # legacy only: node_id → QueuePolicy
        self._last_spawn_t: dict[str, float] = {}

        # BSIM-104-108: tier-compatibility state
        self._tier_defs: dict[str, TierProfile] = {t.name: t for t in (cfg.tiers or [])}
        self._centroid_tier_config: dict[str, dict] = centroid_tier_config or {}
        self._job_compatible_tiers: dict[str, list[str]] = {}  # job_id → resolved tier set
        self._node_tier_name: dict[str, str] = {}              # node_id → tier name

    def _setup(self, env: simpy.Environment) -> None:
        self._env = env
        if self.cfg.time_window_policy:
            windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
            self._active_window_idx = self._find_window_idx(env.now % 86400.0, windows)
            env.process(self._policy_timer(env, windows))
        env.process(self._scale_out_monitor(env))

    # -----------------------------------------------------------------------
    # BSIM-84: time-window helpers
    # -----------------------------------------------------------------------

    def _find_window_idx(self, tod_s: float, windows: list) -> int:
        """Return the index of the window that covers time-of-day tod_s."""
        for i, w in enumerate(windows):
            if w.start_time_s <= tod_s < w.end_time_s:
                return i
        return len(windows) - 1

    def _active_window(self) -> Optional[TimeWindowPolicy]:
        """Return the currently active TimeWindowPolicy, or None."""
        if not self.cfg.time_window_policy:
            return None
        windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
        return windows[self._active_window_idx]

    def _get_active_queue_policy(self, tier_name: str) -> Optional[QueuePolicy]:
        """Return the current window's QueuePolicy for this tier, or None if dormant."""
        window = self._active_window()
        if window is None:
            return None
        for qp in window.queues:
            if qp.is_named and qp.name == tier_name:
                return qp
        return None

    def _route_to_queue(self, peak_ram_gb: float) -> Optional[QueuePolicy]:
        """Legacy: find QueuePolicy by RAM band in the active window."""
        window = self._active_window()
        if window is None:
            return None
        for q in window.queues:
            if not q.is_named and q.exclusive_min_gb < peak_ram_gb <= q.inclusive_max_gb:
                return q
        return None

    def _resolve_compatible_tiers(self, job: JobSpec) -> list[str]:
        """BSIM-106: Resolve the compatible-tier SET for a job at current sim time.

        Priority order:
          1. Time-window override on the centroid (string set, or per-bin set)
          2. job.compatible_tiers set at generation time
          3. Centroid-level default from metadata (string set)
          4. Burst-derived inference: every tier whose spike_max_gb >= job burst
        """
        if not self._tier_defs:
            return []
        centroid_cfg = self._centroid_tier_config.get(job.centroid_id, {})
        if hasattr(self, '_env'):
            now_tod = self._env.now % 86400.0
            for wo in centroid_cfg.get("window_overrides", []):
                if wo["start_time_s"] <= now_tod < wo["end_time_s"]:
                    if wo.get("compatible_tiers"):
                        return list(wo["compatible_tiers"])
                    by_bin = wo.get("compatible_tiers_by_bin")
                    if (by_bin is not None and job.bin_idx is not None
                            and job.bin_idx < len(by_bin)):
                        return list(by_bin[job.bin_idx])
        if job.compatible_tiers:
            return list(job.compatible_tiers)
        cd = centroid_cfg.get("compatible_tiers")
        if cd:
            return list(cd)
        # Fallback: derive from burst arithmetic (BSIM-105 inference path)
        min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
        return [name for name, t in self._tier_defs.items() if t.spike_max_gb >= min_spike]

    def _policy_timer(self, env: simpy.Environment, windows: list[Any]) -> Generator[Any, None, None]:
        """Process: wakes at each window boundary and swaps the active policy."""
        while True:
            current_idx = self._active_window_idx
            current_window = windows[current_idx]
            next_boundary_tod = current_window.end_time_s

            current_tod = env.now % 86400.0
            if next_boundary_tod > current_tod:
                delay = next_boundary_tod - current_tod
            else:
                delay = 86400.0 - current_tod + next_boundary_tod

            yield env.timeout(delay)

            new_idx = (current_idx + 1) % len(windows)
            old_w = windows[current_idx]
            new_w = windows[new_idx]
            self.metrics.policy_swap(env.now, old_w.start_time_s, new_w.start_time_s)
            self._active_window_idx = new_idx

            for proc in list(self._drain_monitors.values()):
                if proc.is_alive:
                    proc.interrupt("policy_swap")

            self._try_schedule(env)

    # -----------------------------------------------------------------------
    # BSIM-85: drain rule helpers
    # -----------------------------------------------------------------------

    def _best_drain_rule(self, node_id: str, idle_vcpu: float) -> Optional[DrainRule]:
        """Return the most aggressive drain rule that applies (highest threshold ≤ idle_vcpu)."""
        if self._tier_defs:
            # Tier mode: look up current window's drain rules for this node's tier
            node_tier = self._node_tier_name.get(node_id)
            if node_tier is None:
                return None
            qpol = self._get_active_queue_policy(node_tier)
            if qpol is None or not qpol.drain_rules:
                return None
            applicable = [r for r in qpol.drain_rules if idle_vcpu >= r.idle_vcpu]
        else:
            # Legacy: use the QueuePolicy recorded at node launch time
            qp = self._node_queue_policy.get(node_id)
            if qp is None or not qp.drain_rules:
                return None
            applicable = [r for r in qp.drain_rules if idle_vcpu >= r.idle_vcpu]
        if not applicable:
            return None
        return max(applicable, key=lambda r: r.idle_vcpu)

    def _drain_monitor(self, env: simpy.Environment, node: NodeModel) -> Generator[Any, None, None]:
        """
        Per-node process: evaluates drain rules continuously.
        Interrupted whenever a job is placed or completes on this node, or
        when the active policy swaps.
        """
        while node.state not in (NodeStateEnum.TERMINATED, NodeStateEnum.DRAINING):
            idle_vcpu = node.physical_vcpu - node.allocated_vcpu
            rule = self._best_drain_rule(node.node_id, idle_vcpu)

            if rule is None:
                try:
                    yield env.timeout(float('inf'))
                except simpy.Interrupt:
                    continue
            else:
                try:
                    yield env.timeout(rule.duration_s)
                except simpy.Interrupt:
                    continue

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
    # Capacity helpers (BSIM-102/104)
    # -----------------------------------------------------------------------

    def _k8s_capacity(self, instance: InstanceTypeConfig, tier_name: str = "") -> K8SCapacityProfile:
        cache_key = (instance.name, tier_name)
        if cache_key not in self._capacity_cache:
            if tier_name and tier_name in self._tier_defs:
                spike_max = self._tier_defs[tier_name].spike_max_gb
            else:
                # Legacy: derive from centroid_peak_rams
                fitting = [r for r in self.centroid_peak_rams if r > 0]
                spike_max = max(fitting) if fitting else 0.0
            self._capacity_cache[cache_key] = compute_k8s_capacity(
                instance=instance, spike_max_gb=spike_max,
                os_overhead_gb=self.cfg.os_overhead_gb)
        return self._capacity_cache[cache_key]

    # -----------------------------------------------------------------------
    # Tier-compatibility helpers (BSIM-106)
    # -----------------------------------------------------------------------

    def _node_compatible(self, node_id: str, job_tiers: list[str]) -> bool:
        """True if the node may host a job with this compatible-tier set."""
        if not self._tier_defs:
            return True  # legacy / no-tier mode: any node is a candidate
        return self._node_tier_name.get(node_id) in job_tiers

    def _viable_tiers(self, job: JobSpec, tiers: list[str]) -> list[str]:
        """Tiers from the set whose spike_max_gb can host the job's burst."""
        min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
        return [t for t in tiers
                if t in self._tier_defs and self._tier_defs[t].spike_max_gb >= min_spike]

    def _pick_launch_tier(self, job: JobSpec) -> Optional[str]:
        """Choose the least-wasteful viable tier for launching a node for this job:
        smallest spike_max_gb that still accommodates the job's burst."""
        viable = self._viable_tiers(job, self._job_compatible_tiers.get(job.job_id, []))
        if not viable:
            return None
        return min(viable, key=lambda t: self._tier_defs[t].spike_max_gb)

    # -----------------------------------------------------------------------
    # Job lifecycle
    # -----------------------------------------------------------------------

    def on_job_arrival(self, env: simpy.Environment, job: JobSpec, arrival_time: float) -> None:
        if not hasattr(self, "_env"): self._setup(env)
        # BSIM-106: resolve compatible-tier set at arrival time
        tiers = self._resolve_compatible_tiers(job)
        if self._tier_defs:
            # BSIM-108: admission — drop declared tiers that cannot host the burst;
            # reject outright if none can.
            viable = self._viable_tiers(job, tiers)
            incompatible = [t for t in tiers if t not in viable]
            if incompatible:
                min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
                self.metrics.record(SimEvent(EventType.TIER_COMPATIBILITY_WARN, env.now, {
                    "job_id": job.job_id, "centroid_id": job.centroid_id,
                    "incompatible_tiers": incompatible, "min_spike_gb": round(min_spike, 3),
                }))
            if not viable:
                min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
                self.metrics.record(SimEvent(EventType.ADMISSION_REJECTED, env.now, {
                    "job_id": job.job_id, "centroid_id": job.centroid_id,
                    "compatible_tiers": tiers, "min_spike_gb": round(min_spike, 3),
                }))
                self.metrics.panic_trigger(env.now, job.job_id, 0.0)
                return
            tiers = viable
            self._job_compatible_tiers[job.job_id] = tiers
        self.metrics.job_queued(env.now, job.job_id, job.centroid_id, "NORMAL",
                                queue_name=(";".join(tiers) if tiers else None))
        self._queue.enqueue(job, arrival_time=arrival_time, priority=Priority.NORMAL, enqueue_time=env.now)
        proc = env.process(self._panic_monitor(env, job, enqueue_time=env.now))
        self._panic_monitors[job.job_id] = proc
        self._try_schedule(env)

    def on_job_complete(self, env: simpy.Environment, node: NodeModel, job: JobSpec) -> None:
        soft = job.profile.soft_limit_ram_gb
        node.allocated_ram_gb = max(0.0, node.allocated_ram_gb - soft)
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - (getattr(job, "soft_cpu", 0) or job.profile.workhorse_declared_vcpu))
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        self._job_compatible_tiers.pop(job.job_id, None)

        pool = self._storage_pools.get(node.node_id)
        if pool is not None:
            pool.job_exit(env.now, job.job_id, workspace_gb(job), self.metrics)

        if node.job_count == 0:
            if node.state == NodeStateEnum.DRAINING:
                # BSIM-85: last job gone from a draining node — terminate immediately
                node.state = NodeStateEnum.TERMINATED
                self.metrics.node_terminated(env.now, node.node_id, 0)
                accruer = self._accruers.get(node.node_id)
                if accruer:
                    accruer.terminate(env.now)
                if pool is not None:
                    pool.close(env.now)
                self._node_tier_name.pop(node.node_id, None)
            else:
                node.state = NodeStateEnum.IDLE; node.idle_since = env.now
                self.metrics.node_idle(env.now, node.node_id)
                env.process(self._idle_timer(env, node))

        self._interrupt_drain_monitor(node.node_id)
        self._try_schedule(env)

    def guarantee_capacity(self, env: simpy.Environment, job: JobSpec) -> None:
        soft = job.profile.soft_limit_ram_gb
        vcpu = (getattr(job, "soft_cpu", 0) or job.profile.workhorse_declared_vcpu)
        job_tiers = self._job_compatible_tiers.get(job.job_id, [])
        for node in self._nodes.values():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node.node_id not in self._reserved
                    and self._node_compatible(node.node_id, job_tiers)
                    and self._k8s_fits(node, soft, vcpu)):
                self._reserved[node.node_id] = job.job_id; return

        if self._tier_defs:
            tier = self._pick_launch_tier(job)
            if tier is None:
                return
            instance = self.registry.get_by_name(self._tier_defs[tier].spawn_instance_class)
            if instance:
                env.process(self._launch_node(env, instance, for_job=job, tier_name=tier))
        else:
            instance = self._select_instance_for_job(job)
            if instance:
                env.process(self._launch_node(env, instance, for_job=job))

    def _select_instance_for_job(self, job: JobSpec) -> Optional[InstanceTypeConfig]:
        """
        Legacy / no-tier instance selection.
        Legacy time_window_policy: use the matched band's spawn_instance_class.
        No-policy: cheapest_fitting.
        """
        if self.cfg.time_window_policy:
            qp = self._route_to_queue(job.profile.preprocess_peak_ram_gb)
            if qp is None:
                return None
            band_key = f"{qp.exclusive_min_gb}-{qp.inclusive_max_gb}"
            now = getattr(self, '_env', None)
            if now is not None:
                now = self._env.now
                cool_down = 60.0 / qp.spawn_rate_per_min
                last = self._last_spawn_t.get(band_key, -cool_down)
                if now - last < cool_down:
                    return None
                self._last_spawn_t[band_key] = now
            return self.registry.get_by_name(qp.spawn_instance_class)
        else:
            soft_gb = job.profile.soft_limit_ram_gb
            vcpu    = (getattr(job, "soft_cpu", 0) or job.profile.workhorse_declared_vcpu)
            for inst in sorted(self.registry.all_types, key=lambda i: i.hourly_price_usd):
                if inst.vcpu < vcpu:
                    continue
                if self._k8s_capacity(inst).effective_schedulable_gb >= soft_gb:
                    return inst
            return None

    def _try_schedule(self, env: simpy.Environment) -> None:
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

    def _place_job(self, env: simpy.Environment, entry: QueueEntry) -> bool:
        job = entry.job; p = job.profile
        soft = p.soft_limit_ram_gb; vcpu = (getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu)
        job_tiers = self._job_compatible_tiers.get(job.job_id, [])
        best = self._best_fit_node(soft, vcpu, job.job_id, job_tiers)
        if best is None: return False
        mon = self._panic_monitors.pop(job.job_id, None)
        if mon and mon.is_alive: mon.interrupt("placed")
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += soft; best.allocated_vcpu += vcpu
        if best.state == NodeStateEnum.IDLE: best.state = NodeStateEnum.READY
        pool = self._storage_pools.get(best.node_id)
        if pool is not None:
            pool.job_start(env.now, job.job_id, workspace_gb(job), self.metrics)
        self._interrupt_drain_monitor(best.node_id)
        env.process(run_job_process(env=env, job=job, node=best, metrics=self.metrics,
            overload_handler=self._overload_handler, arrival_time=entry.arrival_time,
            queue_entry_time=entry.enqueue_time, scheduler=self))
        return True

    def _k8s_fits(self, node: NodeModel, soft_gb: float, vcpu: float) -> bool:
        node_t = self._node_tier_name.get(node.node_id, "")
        cap = self._capacity_cache.get((node.instance.name, node_t))
        if cap is None or cap.effective_schedulable_gb <= 0: return False
        return (node.allocated_ram_gb + soft_gb <= cap.effective_schedulable_gb
                and node.allocated_vcpu + vcpu <= node.physical_vcpu)

    def _best_fit_node(self, soft_gb: float, vcpu: float, job_id: str,
                       job_tiers: list[str]) -> Optional[NodeModel]:
        candidates = [(node.allocated_ram_gb, node)
            for node in self._nodes.values()
            if node.state == NodeStateEnum.READY
            and self._reserved.get(node.node_id, job_id) == job_id
            and self._node_compatible(node.node_id, job_tiers)
            and self._k8s_fits(node, soft_gb, vcpu)]
        if not candidates: return None
        return max(candidates, key=lambda x: x[0])[1]

    def _launch_node(self, env: simpy.Environment, instance: InstanceTypeConfig,
                     for_job: Optional[JobSpec] = None,
                     tier_name: Optional[str] = None) -> Generator[Any, None, None]:
        node_id = str(uuid.uuid4())[:8]
        self.metrics.node_launching(env.now, node_id, instance.name, tier_name=tier_name)
        cap = self._k8s_capacity(instance, tier_name or "")
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.cfg.os_overhead_gb)
        self._nodes[node_id] = node
        if tier_name:
            self._node_tier_name[node_id] = tier_name
        self._capacity_cache[(instance.name, tier_name or "")] = cap
        self._accruers[node_id] = NodeCostAccruer(node_id=node_id, instance=instance, launch_time=env.now)
        if self.cfg.storage is not None:
            pool = GenerationalStoragePool(
                node_id=node_id, config=self.cfg.storage,
                instance=instance, open_time=env.now)
            pool.announce(env.now, self.metrics)
            self._storage_pools[node_id] = pool

        # Legacy: record which band-queue policy launched this node (for drain rules)
        if self.cfg.time_window_policy and not self._tier_defs and for_job is not None:
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

    def _scale_out_monitor(self, env: simpy.Environment) -> Generator[Any, None, None]:
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

    def _provision_to_demand(self, env: simpy.Environment) -> None:
        if self._tier_defs:
            self._provision_to_demand_joint(env)
        else:
            self._provision_to_demand_legacy(env)

    # -----------------------------------------------------------------------
    # BSIM-107: joint provisioner
    # -----------------------------------------------------------------------

    def _provision_to_demand_joint(self, env: simpy.Environment) -> None:
        """Group pending jobs by spawn_instance_class; for each group choose the tier
        configuration that packs the most eligible jobs per node, launch it, repeat
        until overflow is cleared or per-tier spawn-rate / max_nodes limits are hit.

        A job whose burst only fits a larger-spike tier is eligible only for tiers
        that can host it; preferring smaller spike_max_gb on ties keeps bin-packing
        headroom maximal when the job mix allows.
        """
        window = self._active_window()
        active_tiers: dict[str, QueuePolicy] = {}
        if window is not None:
            active_tiers = {
                qp.name: qp for qp in window.queues
                if qp.is_named and qp.name is not None
            }
        else:
            # No time-window policy: every declared tier is implicitly active with
            # default behavioural settings.
            active_tiers = {name: QueuePolicy(name=name) for name in self._tier_defs}

        # Collect pending jobs with their resolved viable tier sets.
        pending: list[tuple[float, float, float, list[str]]] = []  # (peak, soft, vcpu, tiers)
        for entry in sorted(self._queue._heap):
            job = entry.job
            tiers = self._viable_tiers(job, self._job_compatible_tiers.get(job.job_id, []))
            tiers = [t for t in tiers if t in active_tiers]
            if not tiers:
                continue  # dormant or no viable active tier — accumulate, do not provision
            p = job.profile
            soft = p.soft_limit_ram_gb
            vcpu = getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu
            pending.append((p.peak_ram_gb, soft, vcpu, tiers))

        if not pending:
            return

        # Subtract capacity already available on existing ready/launching nodes,
        # tier by tier. A pending job can be absorbed by any existing node whose
        # tier is in its viable set.
        virtual: dict[str, list[list[float]]] = {}  # tier → [[free_gb, free_vcpu], ...]
        for n in self._nodes.values():
            if n.state not in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING):
                continue
            tname = self._node_tier_name.get(n.node_id)
            if tname is None:
                continue
            nc = self._capacity_cache.get((n.instance.name, tname))
            if nc and nc.effective_schedulable_gb > 0:
                virtual.setdefault(tname, []).append(
                    [nc.effective_schedulable_gb - n.allocated_ram_gb,
                     n.physical_vcpu - n.allocated_vcpu])

        overflow: list[tuple[float, float, float, list[str]]] = []
        for peak, soft, vcpu, tiers in pending:
            placed = False
            for t in tiers:
                for vn in virtual.get(t, []):
                    if vn[0] >= soft and vn[1] >= vcpu:
                        vn[0] -= soft; vn[1] -= vcpu
                        placed = True
                        break
                if placed:
                    break
            if not placed:
                overflow.append((peak, soft, vcpu, tiers))

        # Per-tier launch accounting.
        current_nodes: dict[str, int] = {}
        for name in active_tiers:
            current_nodes[name] = sum(
                1 for n in self._nodes.values()
                if self._node_tier_name.get(n.node_id) == name
                and n.state != NodeStateEnum.TERMINATED
            )

        # Spawn-rate gate is evaluated once per pass: a tier may start spawning when
        # it is off cool-down relative to its last launch. Once a tier is chosen in
        # this pass it keeps packing its full batch (Karpenter-style), so a deep
        # single-tier backlog does not spuriously diversify across tiers.
        spawn_eligible = {
            tname: (env.now - self._last_spawn_t.get(tname, -(60.0 / qpol.spawn_rate_per_min))
                    >= 60.0 / qpol.spawn_rate_per_min)
            for tname, qpol in active_tiers.items()
        }
        launched_this_pass: set[str] = set()

        # Greedily launch nodes until overflow is empty or all tiers are blocked.
        # Per-launch tier choice scores by, in order:
        #   1. jobs packed on this one node (primary objective: fewest nodes)
        #   2. eligible overflow jobs for this tier (favours the tier that can also
        #      serve the more-constrained jobs, so outliers consolidate rather than
        #      stranding onto a dedicated extra node)
        #   3. smaller spike_max_gb (least-wasteful reservation when 1 and 2 tie)
        while overflow:
            best_tier = None
            best_packed: list[int] = []
            best_score = (-1, -1, float("-inf"))
            for tname, qpol in active_tiers.items():
                tdef = self._tier_defs.get(tname)
                if tdef is None:
                    continue
                if not (spawn_eligible.get(tname) or tname in launched_this_pass):
                    continue
                instance = self.registry.get_by_name(tdef.spawn_instance_class)
                if instance is None:
                    continue
                if qpol.max_nodes is not None and current_nodes.get(tname, 0) >= qpol.max_nodes:
                    continue
                cap = self._k8s_capacity(instance, tname)
                if cap.effective_schedulable_gb <= 0:
                    continue
                # Greedily pack overflow jobs eligible for this tier onto one new node.
                rem_gb = cap.effective_schedulable_gb
                rem_vcpu = float(instance.vcpu)
                packed: list[int] = []
                eligible = 0
                for i, (peak, soft, vcpu, tiers) in enumerate(overflow):
                    if tname not in tiers:
                        continue
                    eligible += 1
                    if rem_gb >= soft and rem_vcpu >= vcpu:
                        rem_gb -= soft; rem_vcpu -= vcpu
                        packed.append(i)
                if not packed:
                    continue
                score = (len(packed), eligible, -tdef.spike_max_gb)
                if score > best_score:
                    best_score = score
                    best_tier = tname
                    best_packed = packed

            if best_tier is None:
                break  # no tier can launch (rate-limited, capped, or nothing fits)

            tdef = self._tier_defs[best_tier]
            instance = self.registry.get_by_name(tdef.spawn_instance_class)
            self._last_spawn_t[best_tier] = env.now
            launched_this_pass.add(best_tier)
            current_nodes[best_tier] = current_nodes.get(best_tier, 0) + 1
            env.process(self._launch_node(env, instance, tier_name=best_tier))
            packed_set = set(best_packed)
            overflow = [j for i, j in enumerate(overflow) if i not in packed_set]

    def _provision_to_demand_legacy(self, env: simpy.Environment) -> None:
        """
        Legacy Karpenter-style demand provisioning: score every instance type against
        the full overflow queue and launch whichever type packs the most unserved
        jobs per dollar, repeating until all overflow is covered.
        """
        virtual = []
        for n in self._nodes.values():
            if n.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING):
                cap = self._capacity_cache.get((n.instance.name, ""))
                if cap and cap.effective_schedulable_gb > 0:
                    virtual.append([cap.effective_schedulable_gb - n.allocated_ram_gb,
                                     n.physical_vcpu - n.allocated_vcpu])
        overflow = []
        for entry in sorted(self._queue._heap):
            job = entry.job; p = job.profile
            soft = p.soft_limit_ram_gb
            vcpu = getattr(job, "soft_cpu", 0) or p.workhorse_declared_vcpu
            for vn in virtual:
                if vn[0] >= soft and vn[1] >= vcpu:
                    vn[0] -= soft; vn[1] -= vcpu
                    break
            else:
                overflow.append((p.peak_ram_gb, soft, vcpu))

        while overflow:
            inst = self._select_instance_for_overflow(overflow)
            if inst is None:
                break
            cap = self._k8s_capacity(inst)
            env.process(self._launch_node(env, inst))
            rem_gb, rem_vcpu = cap.effective_schedulable_gb, inst.vcpu
            remaining = []
            for peak, soft, vcpu in overflow:
                if rem_gb >= soft and rem_vcpu >= vcpu:
                    rem_gb -= soft; rem_vcpu -= vcpu
                else:
                    remaining.append((peak, soft, vcpu))
            overflow = remaining

    def _select_instance_for_overflow(self, overflow: list) -> Optional[Any]:
        """Score each instance type by (K8S jobs fitting greedily / hourly rate).
        Ties broken by job count to favour larger instances and fewer nodes."""
        best_inst, best_score = None, (-1.0, -1)
        for inst in self.registry.all_types:
            cap = self._k8s_capacity(inst)
            if cap.effective_schedulable_gb <= 0:
                continue
            rem_gb, rem_vcpu = cap.effective_schedulable_gb, inst.vcpu
            count = 0
            for _, soft, vcpu in sorted(overflow, key=lambda x: x[1], reverse=True):
                if rem_gb >= soft and rem_vcpu >= vcpu:
                    rem_gb -= soft; rem_vcpu -= vcpu
                    count += 1
            if count > 0:
                score = (count, count / inst.hourly_price_usd)
                if score > best_score:
                    best_score = score
                    best_inst = inst
        return best_inst

    def _panic_monitor(self, env: simpy.Environment, job: JobSpec, enqueue_time: float) -> Generator[Any, None, None]:
        try: yield env.timeout(self.cfg.panic_threshold_seconds)
        except simpy.Interrupt: return
        self.metrics.panic_trigger(env.now, job.job_id, env.now - enqueue_time)
        self._queue.elevate_to_urgent(job.job_id)
        self.guarantee_capacity(env, job)

    def _idle_timer(self, env: simpy.Environment, node: NodeModel) -> Generator[Any, None, None]:
        idle_start = env.now
        yield env.timeout(self.cfg.idle_timeout_seconds)
        if node.state == NodeStateEnum.IDLE and node.job_count == 0:
            node.state = NodeStateEnum.TERMINATED
            self.metrics.node_terminated(env.now, node.node_id, env.now - idle_start)
            accruer = self._accruers.get(node.node_id)
            if accruer: accruer.terminate(env.now)
            pool = self._storage_pools.get(node.node_id)
            if pool is not None: pool.close(env.now)
            self._node_tier_name.pop(node.node_id, None)

    def cpu_boost(self, env: simpy.Environment, node: NodeModel, metrics: MetricsCollector) -> None:
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_k8s
        run_cpu_boost_k8s(env, node, metrics)

    @property
    def accruers(self) -> list[NodeCostAccruer]: return list(self._accruers.values())

    @property
    def storage_pools(self) -> list[GenerationalStoragePool]:
        return list(self._storage_pools.values())

    def finalize(self, env: simpy.Environment) -> None:
        for accruer in self._accruers.values():
            if not accruer.is_terminated: accruer.terminate(env.now)
        for pool in self._storage_pools.values():
            pool.close(env.now)

    def capacity_report(self) -> dict[str, dict[str, Any]]:
        result = {}
        for (instance_name, tier_name), cap in self._capacity_cache.items():
            report_key = f"{instance_name}|{tier_name}" if tier_name else instance_name
            result[report_key] = {
                "tier_local_mm_gb": cap.tier_local_mm_gb,
                "effective_schedulable_gb": cap.effective_schedulable_gb,
                "soft_limit_gb": cap.soft_limit_gb,
                "max_schedulable_jobs": cap.max_schedulable_jobs,
                "headroom_pct": round(cap.headroom_pct, 1),
            }
        return result
