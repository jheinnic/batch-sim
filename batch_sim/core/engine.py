"""BSIM-11 through 16: Core simulation engine."""
from __future__ import annotations
import heapq, random, uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Optional, Protocol
import simpy
from batch_sim.generator.job_spec import JobSpec
from batch_sim.metrics.collector import MetricsCollector, PhaseID, EventType, NodeState as NodeStateEnum
from batch_sim.registry.instance_registry import NodeCostAccruer
from batch_sim.core.schemas import InstanceTypeConfig, SchedulerConfig


class Priority(IntEnum):
    URGENT = 0
    NORMAL = 1


@dataclass
class RunningJobSlot:
    job: JobSpec
    current_phase: PhaseID
    phase_peak_ram_gb: float
    effective_vcpu: float


class NodeModel:
    def __init__(self, node_id, instance, metrics, os_overhead_gb=0.0):
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

    @property
    def job_count(self): return len(self._slots)
    def instantaneous_ram_gb(self): return sum(s.phase_peak_ram_gb for s in self._slots.values())
    def instantaneous_vcpu(self): return sum(s.effective_vcpu for s in self._slots.values())
    def is_overloaded(self): return self.instantaneous_ram_gb() > self.physical_ram_gb

    def add_job(self, job, phase, ram_gb, vcpu):
        self._slots[job.job_id] = RunningJobSlot(job=job, current_phase=phase,
                                                   phase_peak_ram_gb=ram_gb, effective_vcpu=vcpu)
    def update_phase(self, job_id, phase, ram_gb, vcpu):
        if job_id in self._slots:
            s = self._slots[job_id]; s.current_phase = phase
            s.phase_peak_ram_gb = ram_gb; s.effective_vcpu = vcpu
    def remove_job(self, job_id): self._slots.pop(job_id, None)
    def phase2_jobs(self):
        return [s.job for s in self._slots.values() if s.current_phase == PhaseID.PREPROCESS]


class OverloadHandler:
    def __init__(self, metrics, scheduler_cfg, replay_queue, rng):
        self.metrics = metrics; self.cfg = scheduler_cfg
        self.replay_queue = replay_queue; self.rng = rng

    def check_and_handle(self, env, node) -> Optional[str]:
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
            copy = victim_job.fresh_copy()
            copy.retry_count = victim_job.retry_count
            self.replay_queue.enqueue(copy, arrival_time=env.now + self.cfg.replay_delay_seconds)
        return victim_id


@dataclass(order=True)
class QueueEntry:
    priority: int
    arrival_time: float
    seq: int
    job: JobSpec = field(compare=False)
    enqueue_time: float = field(compare=False, default=0.0)


class JobQueue:
    def __init__(self): self._heap = []; self._seq = 0

    def enqueue(self, job, arrival_time, priority=Priority.NORMAL, enqueue_time=0.0):
        entry = QueueEntry(priority=priority.value, arrival_time=arrival_time,
                           seq=self._seq, job=job, enqueue_time=enqueue_time)
        self._seq += 1
        heapq.heappush(self._heap, entry)

    def peek(self): return self._heap[0] if self._heap else None
    def pop(self): return heapq.heappop(self._heap)
    def __len__(self): return len(self._heap)
    def __iter__(self): return iter(self._heap)

    def elevate_to_urgent(self, job_id):
        for i, entry in enumerate(self._heap):
            if entry.job.job_id == job_id:
                new = QueueEntry(priority=Priority.URGENT.value, arrival_time=entry.arrival_time,
                                 seq=entry.seq, job=entry.job, enqueue_time=entry.enqueue_time)
                self._heap.pop(i); heapq.heapify(self._heap); heapq.heappush(self._heap, new)
                return True
        return False


def run_job_process(env, job, node, metrics, overload_handler, arrival_time, queue_entry_time, scheduler):
    p = job.profile; job_id = job.job_id
    start_time = env.now; queue_wait_s = start_time - queue_entry_time
    metrics.job_start(env.now, job_id, job.centroid_id, node.node_id)

    metrics.phase_transition(env.now, job_id, PhaseID.DOWNLOAD, node.node_id)
    node.add_job(job, PhaseID.DOWNLOAD, ram_gb=p.download_ram_gb, vcpu=1.0)
    yield env.timeout(p.download_duration_s)

    metrics.phase_transition(env.now, job_id, PhaseID.PREPROCESS, node.node_id)
    node.update_phase(job_id, PhaseID.PREPROCESS, ram_gb=p.preprocess_peak_ram_gb, vcpu=p.preprocess_vcpu)
    victim = overload_handler.check_and_handle(env, node)
    if victim == job_id:
        node.remove_job(job_id); scheduler.on_job_complete(env, node, job); return
    yield env.timeout(p.preprocess_duration_s)
    node.update_phase(job_id, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb, vcpu=0.0)

    metrics.phase_transition(env.now, job_id, PhaseID.WORKHORSE, node.node_id)
    for stage in p.stages:
        node.update_phase(job_id, PhaseID.WORKHORSE, ram_gb=p.workhorse_ram_gb,
                          vcpu=stage.effective_threads)
        yield env.timeout(stage.wall_clock_seconds)

    metrics.phase_transition(env.now, job_id, PhaseID.UPLOAD, node.node_id)
    node.update_phase(job_id, PhaseID.UPLOAD, ram_gb=p.upload_ram_gb, vcpu=1.0)
    yield env.timeout(p.upload_duration_s)

    node.remove_job(job_id)
    metrics.job_complete(t=env.now, job_id=job_id, centroid_id=job.centroid_id,
        node_id=node.node_id, queue_wait_s=queue_wait_s,
        total_elapsed_s=env.now - arrival_time, retry_count=job.retry_count)
    scheduler.on_job_complete(env, node, job)


class SimulationEngine:
    def __init__(self, scheduler, metrics, cfg):
        self.scheduler = scheduler; self.metrics = metrics; self.cfg = cfg
        self.env = simpy.Environment()

    def run(self, event_list):
        for event in event_list.events:
            self.env.process(self._arrival_process(event))
        self.env.run()

    def _arrival_process(self, event):
        yield self.env.timeout(event.arrival_time)
        job = event.to_job_spec()
        self.metrics.job_arrival(self.env.now, job.job_id, job.centroid_id)
        self.scheduler.on_job_arrival(self.env, job, arrival_time=event.arrival_time)
