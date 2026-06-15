"""
BSIM-50: K8S+ / OKD+ Scheduler with DaemonSet semaphore facility.

Models a third scheduling strategy where a node-local semaphore (backed by a
Kubernetes DaemonSet sidecar in production) serializes jobs through Phase 2
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
        # Block until released.  release() uses transfer semantics: it does NOT
        # increment _available when waking a waiter, so no decrement here either.
        event = self._env.event()
        self._waiters.append(event)
        yield event

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


def _acquire_sem(env, sem, job_id):
    """Wrapper generator so SimPy can process the acquire."""
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
        self._sems: dict[str, NodeSemaphore] = {}
        self._accruers: dict[str, NodeCostAccruer] = {}
        self._reserved: dict[str, str] = {}
        self._capacity_cache = {}
        self._panic_monitors = {}
        self._env = None
        self._draining: set = set()

        # Provisioner lifecycle timers (KarpenterProvisioner path)
        self._empty_timer_procs: dict = {}       # node_id → simpy proc
        self._underutilized_timer_procs: dict = {} # node_id → simpy proc
        self._max_ttl_procs: dict = {}           # node_id → simpy proc

        # Time-window drain state (time_window_policy path — kept for backward compat)
        self._drain_procs: dict = {}
        self._drain_accrued: dict = {}
        self._drain_last_active: dict = {}

    def _setup(self, env):
        self._env = env
        env.process(self._scale_out_monitor(env))

    def _k8s_capacity(self, instance):
        if instance.name not in self._capacity_cache:
            self._capacity_cache[instance.name] = compute_k8s_capacity(
                instance=instance,
                centroid_peak_rams=self.centroid_peak_rams,
                os_overhead_gb=self.cfg.k8s_os_overhead_gb,
            )
        return self._capacity_cache[instance.name]

    def _policy_instance_for_job(self, now: float, peak_ram_gb: float,
                                   soft_gb: float, vcpu: int):
        """Return the instance mandated by the active time-window policy, or None.

        Routes by preprocess_peak_ram_gb band, then verifies the policy instance
        actually fits the job's K8S capacity constraints.  Returns None if no
        policy is configured, no band matches, or the policy instance is too small.
        """
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
            if q.exclusive_min_gb < peak_ram_gb <= q.inclusive_max_gb:
                inst = self.registry.get_by_name(q.spawn_instance_class)
                if inst is None:
                    return None
                cap = self._k8s_capacity(inst)
                if (inst.ram_gb >= peak_ram_gb + self.cfg.k8s_os_overhead_gb
                        and inst.vcpu >= vcpu
                        and cap.effective_schedulable_gb >= soft_gb):
                    return inst
                return None  # policy instance too small for this job
        return None  # no band covers this job's RAM

    def _get_queue_for_node(self, now: float, node):
        """Return the active time-window QueuePolicy for this node's instance type, or None."""
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
            if q.spawn_instance_class == node.instance.name:
                return q
        return None

    def _update_drain_state(self, env, node) -> None:
        """Recompute cumulative drain timers whenever node idle vCPU changes.

        Each drain rule accumulates independently: its timer accrues whenever
        idle_vcpu >= rule.idle_vcpu and pauses otherwise.  Progress is never
        lost on less-aggressive rules when a more-aggressive rule briefly
        becomes active.  The node drains when the first rule hits its target.
        """
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

        # Flush accruals for all previously-active thresholds before re-evaluating.
        for th in list(last_active):
            accrued[th] = accrued.get(th, 0.0) + (t - last_active.pop(th))

        # Re-activate thresholds still met by new_idle; reset those no longer met.
        # Continuous semantics: a timer restarts from zero whenever idle_vcpu
        # drops below its threshold, rather than resuming from prior accrual.
        for rule in queue.drain_rules:
            th = rule.idle_vcpu
            if new_idle >= th:
                last_active[th] = t
            else:
                accrued[th] = 0.0  # threshold no longer met → restart from scratch

        # Find the soonest timer that will fire.
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

        # Cancel any running drain process and restart with the new deadline.
        existing = self._drain_procs.get(node_id)
        if existing is not None and existing.is_alive:
            existing.interrupt()
        self._drain_procs[node_id] = None

        if min_remaining is not None:
            self._drain_procs[node_id] = env.process(
                self._drain_timer_proc(env, node, min_remaining)
            )

    def _drain_timer_proc(self, env, node, timeout: float):
        """Wait timeout seconds then drain the node.

        If jobs are still running, marks the node DRAINING so no new jobs land
        and on_job_complete terminates it when the last job clears.  If already
        empty, terminates immediately.
        """
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

        # allocated_vcpu is incremented synchronously by _place_job before this
        # method is called, so it reliably reflects pending+running jobs even
        # before the coroutine's node.add_job() executes at the next SimPy step.
        # node.job_count lags by one step and must not be used here.
        has_jobs = node.allocated_vcpu > 1e-9

        # Empty timer — starts when node has no allocated jobs, cancelled on placement.
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

        # Underutilized timer — starts when busy but below threshold, resets on recovery.
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
            # Guard: a job may have arrived after the timer started (SimPy step lag).
            # allocated_vcpu is synchronously updated, so it's the reliable signal.
            if node.job_count == 0 and node.allocated_vcpu < 1e-9:
                self._do_terminate(env, node)
            # else: spurious fire — _update_node_lifecycle will restart on next event
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
        for other in self._nodes.values():
            if (other.node_id == node.node_id
                    or other.node_id in self._draining
                    or other.state != NodeStateEnum.READY):
                continue
            cap = self._capacity_cache.get(other.instance.name)
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
        min_ram = first_peak + self.cfg.k8s_os_overhead_gb
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
            # Simulate packing: how many overflow jobs fit?
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
                                   vcpu: int, now: float = 0.0):
        """Return the cheapest allowed instance that physically fits this job.

        Priority order for the candidate pool:
          1. provisioner.allowed_instance_types  (KarpenterProvisioner path)
          2. active time-window policy instances  (time_window_policy path)
          3. full registry sorted by price        (no-policy fallback)
        """
        min_ram = peak_ram_gb + self.cfg.k8s_os_overhead_gb
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
        """Instances named in the active window's queues, sorted largest-band first.

        Used as the fallback pool when the job's primary policy instance is too
        small — ensures the fallback stays within policy-approved types.
        """
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
        for q in sorted(active.queues, key=lambda q: q.inclusive_max_gb, reverse=True):
            inst = self.registry.get_by_name(q.spawn_instance_class)
            if inst and inst.name not in seen:
                seen.add(inst.name)
                result.append(inst)
        return result

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
        node.allocated_vcpu = max(0.0, node.allocated_vcpu - (job.soft_cpu or job.profile.workhorse_declared_vcpu))
        self._reserved = {k: v for k, v in self._reserved.items() if v != job.job_id}
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

    def guarantee_capacity(self, env, job):
        soft = job.profile.soft_limit_ram_gb
        vcpu = job.soft_cpu or job.profile.workhorse_declared_vcpu
        for node in self._nodes.values():
            if (node.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and node.node_id not in self._reserved
                    and node.node_id not in self._draining
                    and self._k8s_fits(node, soft, vcpu)):
                self._reserved[node.node_id] = job.job_id
                return
        instance = self._cheapest_fitting_for_job(job.profile.peak_ram_gb, soft, vcpu, now=env.now)
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
        vcpu = job.soft_cpu or p.workhorse_declared_vcpu
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
        if self.cfg.provisioner:
            self._update_node_lifecycle(env, best)
        elif self.cfg.time_window_policy:
            self._update_drain_state(env, best)
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
            and node.node_id not in self._draining
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
        if self.cfg.provisioner:
            p = self.cfg.provisioner
            self._max_ttl_procs[node_id] = env.process(
                self._max_ttl_proc(env, node, p.max_node_ttl_s)
            )
            self._update_node_lifecycle(env, node)  # start empty TTL immediately
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
            # Guard against float precision: sub-microsecond remainders round to
            # the same float as env.now, causing env.timeout(remaining) to schedule
            # an event at the current sim time and loop infinitely.
            if remaining > 1e-6:
                yield env.timeout(remaining)
            else:
                self._provision_to_demand(env)
                yield env.timeout(self.cfg.scale_out_poll_s)

    def _provision_to_demand(self, env):
        virtual = []
        for n in self._nodes.values():
            if (n.state in (NodeStateEnum.READY, NodeStateEnum.LAUNCHING)
                    and n.node_id not in self._draining):
                cap = self._capacity_cache.get(n.instance.name)
                if cap and cap.effective_schedulable_gb > 0:
                    virtual.append([cap.effective_schedulable_gb - n.allocated_ram_gb,
                                    n.physical_vcpu - n.allocated_vcpu])

        if self.cfg.provisioner:
            # Collect all jobs that don't fit existing virtual capacity, then
            # pick optimal instance types for them in one pass (Karpenter-style).
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
                # Remove overflow entries that fit on this new node.
                rem_gb = cap.effective_schedulable_gb
                rem_vcpu = inst.vcpu
                remaining = []
                for peak, soft, vcpu in overflow:
                    if rem_gb >= soft and rem_vcpu >= vcpu:
                        rem_gb -= soft; rem_vcpu -= vcpu
                    else:
                        remaining.append((peak, soft, vcpu))
                overflow = remaining
        else:
            # Legacy greedy first-fit: one node per un-placeable job.
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
                        p.peak_ram_gb, soft, vcpu, now=env.now)
                    if instance:
                        cap = self._k8s_capacity(instance)
                        env.process(self._launch_node(env, instance))
                        virtual.append([cap.effective_schedulable_gb - soft,
                                        instance.vcpu - vcpu])

    def _panic_monitor(self, env, job, enqueue_time):
        try:
            yield env.timeout(self.cfg.panic_threshold_seconds)
        except simpy.Interrupt:
            return
        self.metrics.panic_trigger(env.now, job.job_id, env.now - enqueue_time)
        self._queue.elevate_to_urgent(job.job_id)
        self.guarantee_capacity(env, job)

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

    def cpu_boost(self, env, node, metrics):
        from batch_sim.scheduler.cpu_boost_integration import run_cpu_boost_k8s
        run_cpu_boost_k8s(env, node, metrics)

    @property
    def accruers(self):
        return list(self._accruers.values())

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
