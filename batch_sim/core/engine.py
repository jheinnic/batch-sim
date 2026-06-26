"""BSIM-11 through 16: Core simulation engine."""
from __future__ import annotations
import heapq
from dataclasses import dataclass, field
from typing import Any, Generator, Iterator, Optional
import simpy
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import MetricsCollector, PhaseID, NodeState as NodeStateEnum
from batch_sim.core.schemas import InstanceTypeConfig


@dataclass
class RunningJobSlot:
    job: JobSpec
    current_phase: PhaseID
    phase_peak_ram_gb: float
    effective_vcpu: float
    soft_limit_ram_gb: float = 0.0   # BSIM-61: steady-state cap for Util3
    cpu_change_event: Optional[simpy.Event] = None  # set by stage loop; fired by cpu_boost write back
    remaining_cpu_s: float = 0.0    # updated before each yield; read by cpu_boost for progress logging
    stage_vcpu_cap: float = 0.0     # stage.effective_threads cap; set alongside remaining_cpu_s
    stage_t0: float = 0.0           # BSIM-95: iteration-start timestamp for accurate remaining_cpu_s


class NodeModel:
    def __init__(self, node_id: str, instance: InstanceTypeConfig, metrics: MetricsCollector, os_overhead_gb: float = 0.0):
        self.node_id = node_id
        self.instance = instance
        self.metrics = metrics
        self.os_overhead_gb = os_overhead_gb
        self.physical_ram_gb = instance.ram_gb - os_overhead_gb
        self.physical_vcpu = instance.vcpu
        self._slots: dict[str, RunningJobSlot] = {}
        self.state: NodeStateEnum = NodeStateEnum.LAUNCHING
        self.idle_since: float = -1.0
        self.allocated_ram_gb: float = 0.0
        self.allocated_vcpu: float = 0.0
        # BSIM-61: spike headroom fixed at node launch (0.92 × tier_local_MM)
        self.spike_headroom_gb_at_launch: float = 0.0

    @property
    def job_count(self) -> int: return len(self._slots)
    def instantaneous_ram_gb(self) -> float: return sum(s.phase_peak_ram_gb for s in self._slots.values())
    def instantaneous_vcpu(self) -> float: return sum(s.effective_vcpu for s in self._slots.values())
    def is_overloaded(self) -> bool: return self.instantaneous_ram_gb() > self.physical_ram_gb

    def add_job(self, job: JobSpec, phase: PhaseID, ram_gb: float, vcpu: float, soft_limit_gb: float = 0.0) -> None:
        self._slots[job.job_id] = RunningJobSlot(job=job, current_phase=phase,
                                                   phase_peak_ram_gb=ram_gb, effective_vcpu=vcpu,
                                                   soft_limit_ram_gb=soft_limit_gb)
    def update_phase(self, job_id: str, phase: PhaseID, ram_gb: float, vcpu: float) -> None:
        if job_id in self._slots:
            s = self._slots[job_id]; s.current_phase = phase
            s.phase_peak_ram_gb = ram_gb; s.effective_vcpu = vcpu
    def remove_job(self, job_id: str) -> None: self._slots.pop(job_id, None)
    def phase2_jobs(self) -> list[JobSpec]:
        return [s.job for s in self._slots.values() if s.current_phase == PhaseID.PREPROCESS]


class OverloadHandler:
    def __init__(self, metrics: MetricsCollector, scheduler_cfg: Any, replay_queue: JobQueue, rng: Any) -> None:
        self.metrics = metrics; self.cfg = scheduler_cfg
        self.replay_queue = replay_queue; self.rng = rng

    def check_and_handle(self, env: simpy.Environment, node: NodeModel) -> Optional[str]:
        if not node.is_overloaded(): return None
        phase2 = node.phase2_jobs()
        victim_job = self.rng.choice(phase2) if phase2 else \
            max(node._slots.values(), key=lambda s: s.phase_peak_ram_gb).job
        victim_id = victim_job.job_id
        self.metrics.burst_collision(t=env.now, node_id=node.node_id,
            job_ids=[s.job.job_id for s in node._slots.values()], victim_id=victim_id,
            aggregate_ram_gb=node.instantaneous_ram_gb(), node_ram_gb=node.physical_ram_gb)
        victim_job.retry_count += 1
        self.metrics.job_crash(t=env.now, job_id=victim_id,
            centroid_id=victim_job.centroid_id, node_id=node.node_id,
            retry_count=victim_job.retry_count, reason="memory_overload")
        if victim_job.retry_count > self.cfg.max_retries:
            self.metrics.job_terminal(env.now, victim_id, victim_job.centroid_id)
        else:
            copy = victim_job.fresh_copy()      # copy inherits is_cancelled=False
            copy.retry_count = victim_job.retry_count
            self.replay_queue.enqueue(copy, arrival_time=env.now + self.cfg.replay_delay_seconds)
        victim_job.is_cancelled = True          # set after copy so replay is not pre-cancelled
        return victim_id


@dataclass(order=True)
class QueueEntry:
    arrival_time: float
    seq: int
    job: JobSpec = field(compare=False)
    enqueue_time: float = field(compare=False, default=0.0)


class JobQueue:
    def __init__(self) -> None: self._heap: list[QueueEntry] = []; self._seq = 0

    def enqueue(self, job: JobSpec, arrival_time: float, enqueue_time: float = 0.0) -> None:
        entry = QueueEntry(arrival_time=arrival_time, seq=self._seq,
                           job=job, enqueue_time=enqueue_time)
        self._seq += 1
        heapq.heappush(self._heap, entry)

    def peek(self) -> Optional[QueueEntry]: return self._heap[0] if self._heap else None
    def pop(self) -> QueueEntry: return heapq.heappop(self._heap)
    def __len__(self) -> int: return len(self._heap)
    def __iter__(self) -> Iterator[QueueEntry]: return iter(self._heap)


def run_job_process(
    env: simpy.Environment,
    job: JobSpec,
    node: NodeModel,
    metrics: MetricsCollector,
    overload_handler: OverloadHandler,
    arrival_time: float,
    queue_entry_time: float,
    scheduler: Any,
) -> Generator[Any, None, None]:
    p = job.profile; job_id = job.job_id
    start_time = env.now; queue_wait_s = start_time - queue_entry_time
    metrics.job_start(env.now, job_id, job.centroid_id, node.node_id)

    def _evicted() -> bool:
        """True if this job was crashed by another job's overload check while we were yielded."""
        return job.is_cancelled

    metrics.phase_transition(env.now, job_id, PhaseID.DOWNLOAD, node.node_id)
    node.add_job(job, PhaseID.DOWNLOAD, ram_gb=p.download_ram_gb, vcpu=1.0,
                 soft_limit_gb=p.soft_limit_ram_gb)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.download_duration_s)
    if _evicted():
        node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return

    metrics.phase_transition(env.now, job_id, PhaseID.PREPROCESS, node.node_id)
    node.update_phase(job_id, PhaseID.PREPROCESS, ram_gb=p.preprocess_peak_ram_gb, vcpu=p.preprocess_vcpu)
    scheduler.cpu_boost(env, node, metrics)
    victim = overload_handler.check_and_handle(env, node)
    if victim == job_id:
        node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return
    yield env.timeout(p.preprocess_duration_s)
    if _evicted():
        node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return
    node.update_phase(job_id, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb, vcpu=0.0)

    metrics.phase_transition(env.now, job_id, PhaseID.WORKHORSE, node.node_id)
    for stage in p.stages:
        node.update_phase(job_id, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb,
                          vcpu=stage.effective_threads)
        # Initialise remaining_cpu_s on the slot before cpu_boost reads it for progress logging
        _slot_ref = node._slots.get(job_id)
        if _slot_ref:
            _slot_ref.remaining_cpu_s = stage.cpu_seconds
            _slot_ref.stage_vcpu_cap = max(stage.effective_threads, 1e-6)
        scheduler.cpu_boost(env, node, metrics)
        stage_cap = max(stage.effective_threads, 1e-6)
        remaining_cpu_s = stage.cpu_seconds
        while remaining_cpu_s > 1e-9:
            slot = node._slots.get(job_id)
            if slot is None or _evicted():
                node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return
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
        if _evicted():
            node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return

    metrics.phase_transition(env.now, job_id, PhaseID.UPLOAD, node.node_id)
    node.update_phase(job_id, PhaseID.UPLOAD, ram_gb=p.upload_ram_gb, vcpu=1.0)
    scheduler.cpu_boost(env, node, metrics)
    yield env.timeout(p.upload_duration_s)
    if _evicted():
        node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return

    node.remove_job(job_id)
    metrics.job_complete(t=env.now, job_id=job_id, centroid_id=job.centroid_id,
        node_id=node.node_id, queue_wait_s=queue_wait_s,
        total_elapsed_s=env.now - arrival_time, retry_count=job.retry_count)
    scheduler.on_job_complete(env, node, job)


class SimulationEngine:
    def __init__(self, scheduler: Any, metrics: MetricsCollector, cfg: Any) -> None:
        self.scheduler = scheduler; self.metrics = metrics; self.cfg = cfg
        self.env = simpy.Environment()

    def run(self, event_list: Any, cool_off_seconds: float = 0.0) -> None:
        """
        Run the simulation.

        Arrivals are scheduled from the event list (all within horizon_seconds).
        The SimPy environment runs until the last arrival completes or until
        horizon_seconds + cool_off_seconds, whichever comes first in practice.
        cool_off_seconds gives in-flight jobs time to finish after the last
        arrival without being truncated at the horizon boundary.
        """
        for event in event_list.events:
            self.env.process(self._arrival_process(event))
        # Run until all processes complete naturally; cool off is implicit
        # because no new arrivals are scheduled after horizon_seconds.
        # If a hard cutoff is needed: self.env.run(until=until)
        until = (event_list.metadata.get("horizon_seconds", 0)
                 + cool_off_seconds) if cool_off_seconds > 0 else None
        self.env.run(until=until)

    def _arrival_process(self, event: Any) -> Generator[Any, None, None]:
        yield self.env.timeout(event.arrival_time)
        job = event.to_job_spec()
        self.metrics.job_arrival(self.env.now, job.job_id, job.centroid_id)
        self.scheduler.on_job_arrival(self.env, job, arrival_time=event.arrival_time)
