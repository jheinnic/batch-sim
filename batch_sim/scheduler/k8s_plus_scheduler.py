"""
BSIM-50: K8S+ / OKD+ Scheduler with DaemonSet semaphore facility.

Models a third scheduling strategy where a node-local semaphore (backed by a
Kubernetes DaemonSet sidecar in production) serializes jobs through Phase 2
(the peak-RAM preprocess phase).  Rather than crashing and requeuing on memory
collision, jobs block on the semaphore, wait for a permit, execute Phase 2
safely, then release.  Zero crashes; some semaphore-wait latency instead.

Concurrency limit per node:
  burst_slots = floor(spike_headroom_gb / tier_local_MM)
  where spike_headroom_gb = queue.spike_max_gb (named-queue mode)
  semaphore permits = max(1, burst_slots)   # always at least 1 (mutex)

BSIM-104-108: Tier-compatibility model applied (same as K8SScheduler):
  - per-job compatible-tier SET resolved at arrival via centroid.compatible_tiers
  - capacity uses tier.spike_max_gb; admission drops/rejects burst-incompatible tiers
  - nodes tagged per tier; placement scoped by set membership
  - joint cross-tier provisioning within a shared spawn_instance_class

New metric: SEMAPHORE_WAIT — emitted with job_id, node_id, wait_s.
"""

from __future__ import annotations

import uuid, random
from typing import Optional

import simpy

from batch_sim.core.engine import (
    NodeModel, JobQueue, OverloadHandler, run_job_process
)
from batch_sim.core.schemas import SchedulerConfig, TierProfile, QueuePolicy
from batch_sim.generator.job_spec import JobSpec, PhaseProfile
from batch_sim.metrics.collector import (
    MetricsCollector, NodeState as NodeStateEnum, PhaseID, EventType, SimEvent
)
from batch_sim.registry.instance_registry import (
    InstanceRegistry, NodeCostAccruer, compute_k8s_capacity, K8SCapacityProfile, workspace_gb
)
from batch_sim.core.schemas import InstanceTypeConfig
from batch_sim.scheduler.storage_pool import GenerationalStoragePool
from batch_sim.scheduler.burst_pool import NodeBurstPool


# ---------------------------------------------------------------------------
# Phase-2 burst coordination (BSIM-122: GB-aware NodeBurstPool)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Burst-pool-aware job process (BSIM-122)
# ---------------------------------------------------------------------------

def run_job_process_plus(
    env, job, node, metrics, burst_pool: NodeBurstPool,
    arrival_time, queue_entry_time, scheduler,
):
    """BSIM-122: Phase 2 is gated by the node's GB-aware NodeBurstPool — multiple
    jobs may boost concurrently as long as their combined burst fits the tier's
    spike reservation; otherwise they serialise. No crash possible."""
    p = job.profile
    job_id = job.job_id
    start_time = env.now
    queue_wait_s = start_time - queue_entry_time
    # Burst above the bin-packed soft limit; this is what draws on the spike
    # reservation (matches BSIM-108 admission: burst = preprocess_peak - soft_limit).
    burst_gb = max(0.0, p.preprocess_peak_ram_gb - p.soft_limit_ram_gb)

    metrics.job_start(env.now, job_id, job.centroid_id, node.node_id)

    # Phase 1: Download
    metrics.phase_transition(env.now, job_id, PhaseID.DOWNLOAD, node.node_id)
    node.add_job(job, PhaseID.DOWNLOAD, ram_gb=p.download_ram_gb, vcpu=1.0)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.download_duration_s)

    # Phase 2: Pre-process — acquire burst headroom first (bounded by the
    # reservation; never borrows bin-packing space)
    sem_wait_start = env.now
    yield from burst_pool.acquire(burst_gb)
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
    burst_pool.release(burst_gb)

    # Phase 3: Workhorse — dynamic timing, same as run_job_process in engine.py
    node.update_phase(job_id, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb, vcpu=0.0)
    metrics.phase_transition(env.now, job_id, PhaseID.WORKHORSE, node.node_id)
    for stage in p.stages:
        node.update_phase(job_id, PhaseID.WORKHORSE,
                          ram_gb=p.workhorse_ram_gb, vcpu=stage.effective_threads)
        _slot_ref = node._slots.get(job_id)
        if _slot_ref:
            _slot_ref.remaining_cpu_s = stage.cpu_seconds
            _slot_ref.stage_vcpu_cap = max(stage.effective_threads, 1e-6)
        scheduler.cpu_boost(env, node, metrics)
        stage_cap = max(stage.effective_threads, 1e-6)
        remaining_cpu_s = stage.cpu_seconds
        while remaining_cpu_s > 1e-9:
            slot = node._slots.get(job_id)
            if slot is None:
                break
            current_vcpu = min(max(slot.effective_vcpu, 1e-6), stage_cap)
            cpu_evt = env.event()
            slot.cpu_change_event = cpu_evt
            slot.remaining_cpu_s = remaining_cpu_s
            slot.stage_vcpu_cap = stage_cap
            stage_t0 = env.now
            yield env.timeout(remaining_cpu_s / current_vcpu) | cpu_evt
            elapsed = env.now - stage_t0
            remaining_cpu_s = max(0.0, remaining_cpu_s - elapsed * current_vcpu)
            slot.cpu_change_event = None

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


# ---------------------------------------------------------------------------
# K8S+ Scheduler
# ---------------------------------------------------------------------------

class K8SPlusScheduler:
    """
    K8S scheduler with per-node semaphore gating Phase 2.
    Inherits all packing logic from the K8S strategy;
    overrides the job runner to use the semaphore-aware process.
    """

    def __init__(self, cfg, registry, metrics, centroid_peak_rams,
                 centroid_tier_config: "dict[str, dict] | None" = None,
                 rng=None):
        self.cfg = cfg
        self.registry = registry
        self.metrics = metrics
        self.centroid_peak_rams = centroid_peak_rams
        self.rng = rng or random.Random(42)

        self._queue = JobQueue()
        self._nodes: dict[str, NodeModel] = {}
        self._burst_pools: dict[str, NodeBurstPool] = {}
        self._accruers: dict[str, NodeCostAccruer] = {}
        self._storage_pools: dict[str, GenerationalStoragePool] = {}
        self._reserved: dict[str, str] = {}
        # Cache keyed by (instance_name, queue_name_or_empty_string)
        self._capacity_cache: dict[tuple[str, str], K8SCapacityProfile] = {}
        self._env = None
        self._draining: set = set()

        # Provisioner lifecycle timers (KarpenterProvisioner path)
        self._empty_timer_procs: dict = {}
        self._underutilized_timer_procs: dict = {}
        self._max_ttl_procs: dict = {}

        # Time-window drain state (time_window_policy path — kept for backward compat)
        self._drain_procs: dict = {}
        self._drain_accrued: dict = {}
        self._drain_last_active: dict = {}

        # BSIM-104-108: tier-compatibility state
        self._tier_defs: dict[str, TierProfile] = {t.name: t for t in (cfg.tiers or [])}
        self._centroid_tier_config: dict[str, dict] = centroid_tier_config or {}
        self._job_compatible_tiers: dict[str, list[str]] = {}  # job_id → resolved tier set
        self._node_tier_name: dict[str, str] = {}  # node_id → tier name
        self._last_spawn_t: dict[str, float] = {}

    def _setup(self, env):
        self._env = env
        env.process(self._scale_out_monitor(env))

    # -----------------------------------------------------------------------
    # BSIM-100-103: named-queue helpers
    # -----------------------------------------------------------------------

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
        if self._env is not None:
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
        min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
        return [name for name, t in self._tier_defs.items() if t.spike_max_gb >= min_spike]

    def _viable_tiers(self, job: JobSpec, tiers: list[str]) -> list[str]:
        """Tiers from the set whose spike_max_gb can host the job's burst."""
        min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
        return [t for t in tiers
                if t in self._tier_defs and self._tier_defs[t].spike_max_gb >= min_spike]

    def _pick_launch_tier(self, job: JobSpec) -> Optional[str]:
        """Least-wasteful viable tier for launching a node for this job."""
        viable = self._viable_tiers(job, self._job_compatible_tiers.get(job.job_id, []))
        if not viable:
            return None
        return min(viable, key=lambda t: self._tier_defs[t].spike_max_gb)

    def _node_compatible(self, node_id: str, job_tiers: list[str]) -> bool:
        if not self._tier_defs:
            return True
        return self._node_tier_name.get(node_id) in job_tiers

    def _get_active_queue_policy(self, queue_name: str) -> Optional[QueuePolicy]:
        """Return the current time-window's QueuePolicy for this named queue, or None."""
        if not self.cfg.time_window_policy:
            return None
        now_tod = (self._env.now if self._env else 0.0) % 86400.0
        windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
        active = windows[-1]
        for w in windows:
            if w.start_time_s <= now_tod < w.end_time_s:
                active = w
                break
        for qp in active.queues:
            if qp.is_named and qp.name == queue_name:
                return qp
        return None

    # -----------------------------------------------------------------------
    # Capacity helpers (BSIM-102)
    # -----------------------------------------------------------------------

    def _k8s_capacity(self, instance, queue_name: str = "") -> K8SCapacityProfile:
        cache_key = (instance.name, queue_name)
        if cache_key not in self._capacity_cache:
            if queue_name and queue_name in self._tier_defs:
                spike_max = self._tier_defs[queue_name].spike_max_gb
            else:
                fitting = [r for r in self.centroid_peak_rams if r > 0]
                spike_max = max(fitting) if fitting else 0.0
            self._capacity_cache[cache_key] = compute_k8s_capacity(
                instance=instance,
                spike_max_gb=spike_max,
                os_overhead_gb=self.cfg.os_overhead_gb,
            )
        return self._capacity_cache[cache_key]

    # -----------------------------------------------------------------------
    # Legacy time-window helpers (kept for backward compat)
    # -----------------------------------------------------------------------

    def _policy_instance_for_job(self, now: float, peak_ram_gb: float,
                                   soft_gb: float, vcpu: int):
        """Return the instance mandated by the active time-window policy, or None.

        Named-queue mode: uses centroid's queue_name to find the queue def.
        Legacy mode: routes by preprocess_peak_ram_gb RAM band.
        """
        if self._tier_defs:
            # Tier mode — caller resolves via _job_compatible_tiers / _pick_launch_tier
            return None
        if not self.cfg.time_window_policy:
            return None
        tod = now % 86400.0
        windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
        active = windows[-1]
        for w in windows:
            if w.start_time_s <= tod < w.end_time_s:
                active = w
                break
        for q in active.queues:
            if not q.is_named and q.exclusive_min_gb < peak_ram_gb <= q.inclusive_max_gb:
                inst = self.registry.get_by_name(q.spawn_instance_class)
                if inst is None:
                    return None
                cap = self._k8s_capacity(inst)
                if (inst.ram_gb >= peak_ram_gb + self.cfg.os_overhead_gb
                        and inst.vcpu >= vcpu
                        and cap.effective_schedulable_gb >= soft_gb):
                    return inst
                return None
        return None

    def _get_queue_for_node(self, now: float, node):
        """Return the active time-window QueuePolicy for this node, or None.

        Named-queue mode: looks up node_queue_name and finds active policy.
        Legacy mode: matches by instance type name against active band queues.
        """
        if self._tier_defs:
            nq = self._node_tier_name.get(node.node_id)
            if nq is None:
                return None
            return self._get_active_queue_policy(nq)
        if not self.cfg.time_window_policy:
            return None
        tod = now % 86400.0
        windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
        active = windows[-1]
        for w in windows:
            if w.start_time_s <= tod < w.end_time_s:
                active = w
                break
        for q in active.queues:
            if not q.is_named and q.spawn_instance_class == node.instance.name:
                return q
        return None

    def _update_drain_state(self, env, node) -> None:
        """Recompute cumulative drain timers whenever node idle vCPU changes."""
        if node.state == NodeStateEnum.TERMINATED:
            return
        t = env.now
        node_id = node.node_id
        new_idle = node.physical_vcpu - node.allocated_vcpu

        queue = self._get_queue_for_node(t, node)
        if queue is None or not queue.drain_rules:
            return

        accrued = self._drain_accrued.setdefault(node_id, {})
        last_active = self._drain_last_active.setdefault(node_id, {})

        for th in list(last_active):
            accrued[th] = accrued.get(th, 0.0) + (t - last_active.pop(th))

        for rule in queue.drain_rules:
            th = rule.idle_vcpu
            if new_idle >= th:
                last_active[th] = t
            else:
                accrued[th] = 0.0

        min_remaining = None
        for rule in queue.drain_rules:
            if rule.idle_vcpu not in last_active:
                continue
            remaining = rule.duration_s - accrued.get(rule.idle_vcpu, 0.0)
            if remaining <= 0:
                min_remaining = 0.0
                break
            if min_remaining is None or remaining < min_remaining:
                min_remaining = remaining

        existing = self._drain_procs.get(node_id)
        if existing is not None and existing.is_alive:
            existing.interrupt()
        self._drain_procs[node_id] = None

        if min_remaining is not None:
            self._drain_procs[node_id] = env.process(
                self._drain_timer_proc(env, node, min_remaining)
            )

    def _drain_timer_proc(self, env, node, timeout: float):
        try:
            yield env.timeout(timeout)
            if node.state == NodeStateEnum.TERMINATED:
                return
            if node.job_count == 0:
                self._do_terminate(env, node)
            else:
                self._draining.add(node.node_id)
        except simpy.Interrupt:
            pass

    def _do_terminate(self, env, node):
        """Emit NODE_TERMINATED, set state, and close the cost accruer."""
        if node.state == NodeStateEnum.TERMINATED:
            return
        node.state = NodeStateEnum.TERMINATED
        self._draining.discard(node.node_id)
        node_id = node.node_id
        caller = env.active_process
        for proc_dict in (self._empty_timer_procs, self._underutilized_timer_procs,
                          self._max_ttl_procs, self._drain_procs):
            proc = proc_dict.get(node_id)
            if proc and proc.is_alive and proc is not caller:
                proc.interrupt()
            proc_dict[node_id] = None
        idle_since = node.idle_since if node.idle_since >= 0 else env.now
        self.metrics.node_terminated(env.now, node.node_id, env.now - idle_since)
        accruer = self._accruers.get(node.node_id)
        if accruer:
            accruer.terminate(env.now)
        pool = self._storage_pools.get(node.node_id)
        if pool is not None:
            pool.close(env.now)
        self._node_tier_name.pop(node_id, None)

    # ------------------------------------------------------------------
    # Provisioner lifecycle (KarpenterProvisioner path)
    # ------------------------------------------------------------------

    def _update_node_lifecycle(self, env, node) -> None:
        """Start/cancel empty and underutilized timers based on current node state."""
        if node.state == NodeStateEnum.TERMINATED or node.node_id in self._draining:
            return
        p = self.cfg.provisioner
        node_id = node.node_id
        utilization_pct = (100.0 * node.allocated_vcpu / node.physical_vcpu
                           if node.physical_vcpu > 0 else 0.0)

        has_jobs = node.allocated_vcpu > 1e-9

        empty_proc = self._empty_timer_procs.get(node_id)
        if not has_jobs:
            if empty_proc is None or not empty_proc.is_alive:
                self._empty_timer_procs[node_id] = env.process(
                    self._empty_timer_proc(env, node, p.empty_ttl_s)
                )
        else:
            if empty_proc and empty_proc.is_alive:
                empty_proc.interrupt()
            self._empty_timer_procs[node_id] = None

        under_proc = self._underutilized_timer_procs.get(node_id)
        is_under = has_jobs and utilization_pct < p.underutilize_threshold_pct
        if is_under:
            if under_proc is None or not under_proc.is_alive:
                self._underutilized_timer_procs[node_id] = env.process(
                    self._underutilized_timer_proc(env, node, p.underutilize_ttl_s)
                )
        else:
            if under_proc and under_proc.is_alive:
                under_proc.interrupt()
            self._underutilized_timer_procs[node_id] = None

    def _cancel_lifecycle_timers(self, env, node_id: str) -> None:
        """Cancel resettable timers when a node enters DRAINING."""
        caller = env.active_process
        for proc_dict in (self._empty_timer_procs, self._underutilized_timer_procs):
            proc = proc_dict.get(node_id)
            if proc and proc.is_alive and proc is not caller:
                proc.interrupt()
            proc_dict[node_id] = None

    def _empty_timer_proc(self, env, node, ttl: float):
        try:
            yield env.timeout(ttl)
            if node.state == NodeStateEnum.TERMINATED:
                return
            if node.job_count == 0 and node.allocated_vcpu < 1e-9:
                self._do_terminate(env, node)
        except simpy.Interrupt:
            pass

    def _underutilized_timer_proc(self, env, node, ttl: float):
        try:
            yield env.timeout(ttl)
            if node.state == NodeStateEnum.TERMINATED:
                return
            if node.job_count == 0:
                self._do_terminate(env, node)
            else:
                self._draining.add(node.node_id)
                self._cancel_lifecycle_timers(env, node.node_id)
        except simpy.Interrupt:
            pass

    def _max_ttl_proc(self, env, node, ttl: float):
        try:
            yield env.timeout(ttl)
            if node.state == NodeStateEnum.TERMINATED:
                return
            if node.job_count == 0:
                self._do_terminate(env, node)
            else:
                self._draining.add(node.node_id)
                self._cancel_lifecycle_timers(env, node.node_id)
        except simpy.Interrupt:
            pass

    def _try_consolidate(self, env, node) -> None:
        """Drain node immediately if its remaining load fits on another ready node."""
        if node.state == NodeStateEnum.TERMINATED or node.node_id in self._draining:
            return
        if node.job_count == 0:
            return
        p = self.cfg.provisioner
        utilization_pct = (100.0 * node.allocated_vcpu / node.physical_vcpu
                           if node.physical_vcpu > 0 else 0.0)
        if utilization_pct >= p.consolidation_threshold_pct:
            return
        needed_gb = node.allocated_ram_gb
        needed_vcpu = node.allocated_vcpu
        node_q = self._node_tier_name.get(node.node_id)
        for other in self._nodes.values():
            if (other.node_id == node.node_id
                    or other.node_id in self._draining
                    or other.state != NodeStateEnum.READY):
                continue
            # Only consolidate within the same queue
            if node_q and self._node_tier_name.get(other.node_id) != node_q:
                continue
            other_q = self._node_tier_name.get(other.node_id, "")
            cap = self._capacity_cache.get((other.instance.name, other_q))
            if cap is None:
                continue
            avail_gb = cap.effective_schedulable_gb - other.allocated_ram_gb
            avail_vcpu = other.physical_vcpu - other.allocated_vcpu
            if avail_gb >= needed_gb and avail_vcpu >= needed_vcpu:
                self._draining.add(node.node_id)
                self._cancel_lifecycle_timers(env, node.node_id)
                return

    # ------------------------------------------------------------------
    # Provisioner instance selection
    # ------------------------------------------------------------------

    def _select_instance_for_overflow(self, overflow: list) -> Optional[InstanceTypeConfig]:
        """Pick the instance type that covers the most overflow jobs per dollar/hr.

        overflow is a list of (peak_ram_gb, soft_gb, vcpu) tuples for jobs
        that have no existing capacity.  The first entry is the trigger job
        that must fit; candidates that can't fit it are skipped.
        """
        p = self.cfg.provisioner
        first_peak, first_soft, first_vcpu = overflow[0]
        min_ram = first_peak + self.cfg.os_overhead_gb
        best_score = -1.0
        best_inst = None
        for inst_name in p.allowed_instance_types:
            inst = self.registry.get_by_name(inst_name)
            if inst is None:
                continue
            if inst.ram_gb < min_ram or inst.vcpu < first_vcpu:
                continue
            cap = self._k8s_capacity(inst)
            if cap.effective_schedulable_gb < first_soft:
                continue
            rem_gb = cap.effective_schedulable_gb
            rem_vcpu = inst.vcpu
            count = 0
            for peak, soft, vcpu in overflow:
                if rem_gb >= soft and rem_vcpu >= vcpu:
                    rem_gb -= soft
                    rem_vcpu -= vcpu
                    count += 1
            if count == 0:
                continue
            score = count / inst.hourly_price_usd
            if score > best_score:
                best_score = score
                best_inst = inst
        return best_inst

    def _cheapest_fitting_for_job(self, peak_ram_gb: float, soft_gb: float,
                                   vcpu: int, now: float = 0.0,
                                   tier_name: str = ""):
        """Return the cheapest allowed instance that physically fits this job.

        Tier mode: uses the tier profile's spawn_instance_class.
        Provisioner path: searches allowed_instance_types.
        Legacy time_window_policy path: uses active window band instances.
        No-policy fallback: full registry sorted by price.
        """
        min_ram = peak_ram_gb + self.cfg.os_overhead_gb
        if self._tier_defs and tier_name:
            tdef = self._tier_defs.get(tier_name)
            if tdef is not None:
                inst = self.registry.get_by_name(tdef.spawn_instance_class)
                if inst and inst.ram_gb >= min_ram and inst.vcpu >= vcpu:
                    cap = self._k8s_capacity(inst, tier_name)
                    if cap.effective_schedulable_gb >= soft_gb:
                        return inst
            return None
        if self.cfg.provisioner:
            candidates = [self.registry.get_by_name(n)
                          for n in self.cfg.provisioner.allowed_instance_types]
            candidates = [i for i in candidates if i is not None]
        elif self.cfg.time_window_policy:
            inst = self._policy_instance_for_job(now, peak_ram_gb, soft_gb, vcpu)
            if inst is not None:
                return inst
            candidates = self._policy_instance_candidates(now)
        else:
            candidates = self.registry.all_types
        for inst in candidates:
            if inst.ram_gb < min_ram or inst.vcpu < vcpu:
                continue
            cap = self._k8s_capacity(inst)
            if cap.effective_schedulable_gb >= soft_gb:
                return inst
        return None

    def _policy_instance_candidates(self, now: float):
        """Legacy: instances named in the active window's band queues."""
        if not self.cfg.time_window_policy:
            return self.registry.all_types
        tod = now % 86400.0
        windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
        active = windows[-1]
        for w in windows:
            if w.start_time_s <= tod < w.end_time_s:
                active = w
                break
        seen: set = set()
        result = []
        legacy = [q for q in active.queues if not q.is_named]
        for q in sorted(legacy, key=lambda q: q.inclusive_max_gb or 0.0, reverse=True):
            inst = self.registry.get_by_name(q.spawn_instance_class)
            if inst and inst.name not in seen:
                seen.add(inst.name)
                result.append(inst)
        return result

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def on_job_arrival(self, env, job, arrival_time):
        if not self._env:
            self._setup(env)
        # BSIM-106: resolve compatible-tier set at arrival time
        tiers = self._resolve_compatible_tiers(job)
        if self._tier_defs:
            # BSIM-108: drop declared tiers that cannot host the burst; reject if none can.
            viable = self._viable_tiers(job, tiers)
            incompatible = [t for t in tiers if t not in viable]
            min_spike = job.profile.preprocess_peak_ram_gb - job.profile.soft_limit_ram_gb
            if incompatible:
                self.metrics.record(SimEvent(EventType.TIER_COMPATIBILITY_WARN, env.now, {
                    "job_id": job.job_id, "centroid_id": job.centroid_id,
                    "incompatible_tiers": incompatible, "min_spike_gb": round(min_spike, 3),
                }))
            if not viable:
                self.metrics.record(SimEvent(EventType.ADMISSION_REJECTED, env.now, {
                    "job_id": job.job_id, "centroid_id": job.centroid_id,
                    "compatible_tiers": tiers, "min_spike_gb": round(min_spike, 3),
                }))
                return
            tiers = viable
            self._job_compatible_tiers[job.job_id] = tiers
        self.metrics.job_queued(env.now, job.job_id, job.centroid_id, "NORMAL",
                                queue_name=(";".join(tiers) if tiers else None))
        self._queue.enqueue(job, arrival_time=arrival_time, enqueue_time=env.now)
        self._try_schedule(env)

    def on_job_complete(self, env, node, job):
        soft = job.profile.soft_limit_ram_gb
        node.allocated_ram_gb = max(0.0, node.allocated_ram_gb - soft)
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - (job.soft_cpu or job.profile.workhorse_declared_vcpu))
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        self._job_compatible_tiers.pop(job.job_id, None)
        pool = self._storage_pools.get(node.node_id)
        if pool is not None:
            pool.job_exit(env.now, job.job_id, workspace_gb(job), self.metrics)
        if node.job_count == 0:
            if node.node_id in self._draining:
                self._do_terminate(env, node)
            elif node.state != NodeStateEnum.TERMINATED:
                node.state = NodeStateEnum.IDLE
                node.idle_since = env.now
                self.metrics.node_idle(env.now, node.node_id)
        if node.state != NodeStateEnum.TERMINATED:
            if self.cfg.provisioner:
                self._update_node_lifecycle(env, node)
                if node.job_count > 0:
                    self._try_consolidate(env, node)
            elif self.cfg.time_window_policy:
                self._update_drain_state(env, node)
            elif node.job_count == 0:
                env.process(self._idle_timer_fallback(env, node))
        self._try_schedule(env)

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
        vcpu = job.soft_cpu or p.workhorse_declared_vcpu
        job_tiers = self._job_compatible_tiers.get(job.job_id, [])
        best = self._best_fit_node(soft, vcpu, job.job_id, job_tiers)
        if best is None:
            return False
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
        best.allocated_ram_gb += soft
        best.allocated_vcpu += vcpu
        if best.state == NodeStateEnum.IDLE:
            best.state = NodeStateEnum.READY
        if self.cfg.provisioner:
            self._update_node_lifecycle(env, best)
        elif self.cfg.time_window_policy:
            self._update_drain_state(env, best)
        pool = self._storage_pools.get(best.node_id)
        if pool is not None:
            pool.job_start(env.now, job.job_id, workspace_gb(job), self.metrics)
        bp = self._burst_pools[best.node_id]
        env.process(run_job_process_plus(
            env=env, job=job, node=best, metrics=self.metrics, burst_pool=bp,
            arrival_time=entry.arrival_time, queue_entry_time=entry.enqueue_time,
            scheduler=self,
        ))
        return True

    def _k8s_fits(self, node, soft_gb, vcpu):
        node_q = self._node_tier_name.get(node.node_id, "")
        cap = self._capacity_cache.get((node.instance.name, node_q))
        if cap is None or cap.effective_schedulable_gb <= 0:
            return False
        return (node.allocated_ram_gb + soft_gb <= cap.effective_schedulable_gb
                and node.allocated_vcpu + vcpu <= node.physical_vcpu)

    def _best_fit_node(self, soft_gb, vcpu, job_id, job_tiers):
        candidates = [
            (node.allocated_ram_gb, node)
            for node in self._nodes.values()
            if node.state == NodeStateEnum.READY
            and node.node_id not in self._draining
            and self._reserved.get(node.node_id, job_id) == job_id
            and self._node_compatible(node.node_id, job_tiers)
            and self._k8s_fits(node, soft_gb, vcpu)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda x: x[0])[1]

    def _launch_node(self, env, instance, for_job=None, tier_name=None):
        node_id = str(uuid.uuid4())[:8]
        effective_tier = tier_name
        if effective_tier is None and for_job is not None:
            picked = self._pick_launch_tier(for_job) if self._tier_defs else None
            effective_tier = picked or ""
        effective_tier = effective_tier or ""
        self.metrics.node_launching(env.now, node_id, instance.name,
                                    tier_name=effective_tier or None)
        cap = self._k8s_capacity(instance, effective_tier)
        node = NodeModel(node_id=node_id, instance=instance, metrics=self.metrics,
                         os_overhead_gb=self.cfg.os_overhead_gb)
        self._nodes[node_id] = node
        # BSIM-122: GB-aware burst pool sized to this node's spike reservation
        # (tier spike_max_gb in tier mode; derived spike headroom in legacy mode).
        self._burst_pools[node_id] = NodeBurstPool(
            env=env, node_physical_ram_gb=instance.ram_gb,
            os_overhead_gb=self.cfg.os_overhead_gb,
            headroom_gb=cap.spike_headroom_gb)
        if effective_tier:
            self._node_tier_name[node_id] = effective_tier
        self._capacity_cache[(instance.name, effective_tier)] = cap
        self._accruers[node_id] = NodeCostAccruer(
            node_id=node_id, instance=instance, launch_time=env.now)
        if self.cfg.storage is not None:
            pool = GenerationalStoragePool(
                node_id=node_id, config=self.cfg.storage,
                instance=instance, open_time=env.now)
            pool.announce(env.now, self.metrics)
            self._storage_pools[node_id] = pool
        yield env.timeout(self.cfg.warmup_delay_seconds)
        node.state = NodeStateEnum.READY
        self.metrics.node_ready(env.now, node_id, instance.name)
        if self.cfg.provisioner:
            p = self.cfg.provisioner
            self._max_ttl_procs[node_id] = env.process(
                self._max_ttl_proc(env, node, p.max_node_ttl_s)
            )
            self._update_node_lifecycle(env, node)
        elif self.cfg.time_window_policy:
            self._update_drain_state(env, node)
        if for_job:
            self._reserved[node_id] = for_job.job_id
        self._try_schedule(env)

    def _scale_out_monitor(self, env):
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
        if self._tier_defs:
            self._provision_to_demand_joint(env)
        elif self.cfg.provisioner:
            self._provision_to_demand_karpenter(env)
        else:
            self._provision_to_demand_legacy(env)

    def _provision_to_demand_joint(self, env) -> None:
        """BSIM-107: joint cross-tier provisioning.

        Group pending jobs by spawn_instance_class; for each launch choose the tier
        configuration that packs the most eligible jobs onto one new node, preferring
        smaller spike_max_gb on ties to maximise bin-packing headroom.
        """
        # Determine active tiers for the current window (all tiers active when no policy).
        active_tiers: dict[str, QueuePolicy] = {}
        if self.cfg.time_window_policy:
            now_tod = (self._env.now if self._env else 0.0) % 86400.0
            windows = sorted(self.cfg.time_window_policy, key=lambda w: w.start_time_s)
            active_w = windows[-1]
            for w in windows:
                if w.start_time_s <= now_tod < w.end_time_s:
                    active_w = w
                    break
            active_tiers = {qp.name: qp for qp in active_w.queues
                            if qp.is_named and qp.name is not None}
        else:
            active_tiers = {name: QueuePolicy(name=name) for name in self._tier_defs}

        # Pending jobs with their viable, active tier sets.
        pending: list[tuple[float, float, float, list[str]]] = []
        for entry in sorted(self._queue._heap):
            job = entry.job
            tiers = self._viable_tiers(job, self._job_compatible_tiers.get(job.job_id, []))
            tiers = [t for t in tiers if t in active_tiers]
            if not tiers:
                continue
            p = job.profile
            soft = p.soft_limit_ram_gb
            vcpu = job.soft_cpu or p.workhorse_declared_vcpu
            pending.append((p.peak_ram_gb, soft, vcpu, tiers))

        if not pending:
            return

        # Subtract existing per-tier free capacity.
        virtual: dict[str, list[list[float]]] = {}
        for n in self._nodes.values():
            if (n.state not in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    or n.node_id in self._draining):
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

        current_nodes: dict[str, int] = {
            name: sum(1 for n in self._nodes.values()
                      if self._node_tier_name.get(n.node_id) == name
                      and n.state != NodeStateEnum.TERMINATED)
            for name in active_tiers
        }

        # Spawn-rate gate evaluated once per pass; a chosen tier keeps packing its
        # full batch (see K8SScheduler._provision_to_demand_joint for rationale).
        spawn_eligible = {
            tname: (env.now - self._last_spawn_t.get(tname, -(60.0 / qpol.spawn_rate_per_min))
                    >= 60.0 / qpol.spawn_rate_per_min)
            for tname, qpol in active_tiers.items()
        }
        launched_this_pass: set = set()

        # Per-launch tier choice scores by: (1) jobs packed on one node, (2) eligible
        # overflow for this tier (consolidate outliers rather than stranding them),
        # (3) smaller spike_max_gb (least waste).
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
                break

            tdef = self._tier_defs[best_tier]
            instance = self.registry.get_by_name(tdef.spawn_instance_class)
            self._last_spawn_t[best_tier] = env.now
            launched_this_pass.add(best_tier)
            current_nodes[best_tier] = current_nodes.get(best_tier, 0) + 1
            env.process(self._launch_node(env, instance, tier_name=best_tier))
            packed_set = set(best_packed)
            overflow = [j for i, j in enumerate(overflow) if i not in packed_set]

    def _provision_to_demand_karpenter(self, env) -> None:
        """Karpenter provisioner path: overflow-score instance selection."""
        virtual = []
        for n in self._nodes.values():
            if (n.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and n.node_id not in self._draining):
                nq = self._node_tier_name.get(n.node_id, "")
                cap = self._capacity_cache.get((n.instance.name, nq))
                if cap and cap.effective_schedulable_gb > 0:
                    virtual.append([cap.effective_schedulable_gb - n.allocated_ram_gb,
                                    n.physical_vcpu - n.allocated_vcpu])

        overflow = []
        for entry in sorted(self._queue._heap):
            job = entry.job; p = job.profile
            soft = p.soft_limit_ram_gb
            vcpu = job.soft_cpu or p.workhorse_declared_vcpu
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
            rem_gb = cap.effective_schedulable_gb
            rem_vcpu = inst.vcpu
            remaining = []
            for peak, soft, vcpu in overflow:
                if rem_gb >= soft and rem_vcpu >= vcpu:
                    rem_gb -= soft; rem_vcpu -= vcpu
                else:
                    remaining.append((peak, soft, vcpu))
            overflow = remaining

    def _provision_to_demand_legacy(self, env) -> None:
        """Legacy greedy first-fit: one node per un-placeable job."""
        virtual = []
        for n in self._nodes.values():
            if (n.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and n.node_id not in self._draining):
                nq = self._node_tier_name.get(n.node_id, "")
                cap = self._capacity_cache.get((n.instance.name, nq))
                if cap and cap.effective_schedulable_gb > 0:
                    virtual.append([cap.effective_schedulable_gb - n.allocated_ram_gb,
                                    n.physical_vcpu - n.allocated_vcpu])

        for entry in sorted(self._queue._heap):
            job = entry.job; p = job.profile
            soft = p.soft_limit_ram_gb
            vcpu = job.soft_cpu or p.workhorse_declared_vcpu
            for vn in virtual:
                if vn[0] >= soft and vn[1] >= vcpu:
                    vn[0] -= soft; vn[1] -= vcpu
                    break
            else:
                instance = self._cheapest_fitting_for_job(
                    p.peak_ram_gb, soft, vcpu, now=env.now, tier_name="")
                if instance:
                    cap = self._k8s_capacity(instance, "")
                    env.process(self._launch_node(env, instance))
                    virtual.append([cap.effective_schedulable_gb - soft,
                                    instance.vcpu - vcpu])

    def _idle_timer_fallback(self, env, node):
        """Simple flat idle timer used only when no time_window_policy is configured."""
        idle_start = env.now
        yield env.timeout(self.cfg.idle_timeout_seconds)
        if node.state == NodeStateEnum.IDLE and node.job_count == 0:
            node.state = NodeStateEnum.TERMINATED
            self.metrics.node_terminated(env.now, node.node_id, env.now - idle_start)
            accruer = self._accruers.get(node.node_id)
            if accruer:
                accruer.terminate(env.now)
            pool = self._storage_pools.get(node.node_id)
            if pool is not None:
                pool.close(env.now)
            self._node_tier_name.pop(node.node_id, None)

    def cpu_boost(self, env, node, metrics):
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_k8s
        run_cpu_boost_k8s(env, node, metrics)

    @property
    def accruers(self):
        return list(self._accruers.values())

    @property
    def storage_pools(self) -> list[GenerationalStoragePool]:
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

    def capacity_report(self):
        result = {}
        for (instance_name, queue_name), cap in self._capacity_cache.items():
            report_key = f"{instance_name}|{queue_name}" if queue_name else instance_name
            result[report_key] = {
                'tier_local_mm_gb': cap.tier_local_mm_gb,
                'effective_schedulable_gb': cap.effective_schedulable_gb,
                'soft_limit_gb': cap.soft_limit_gb,
                'max_schedulable_jobs': cap.max_schedulable_jobs,
                'headroom_pct': round(cap.headroom_pct, 1),
                # BSIM-122: burst concurrency is GB-bounded by the reservation,
                # not a fixed permit count.
                'burst_headroom_gb': round(cap.spike_headroom_gb, 2),
            }
        return result

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
