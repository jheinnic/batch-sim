"""BSIM-31: Typed simulation events and central metrics collector."""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class EventType(str, Enum):
    JOB_ARRIVAL      = "job_arrival"
    JOB_QUEUED       = "job_queued"
    JOB_START        = "job_start"
    PHASE_TRANSITION = "phase_transition"
    JOB_COMPLETE     = "job_complete"
    JOB_CRASH        = "job_crash"
    JOB_TERMINAL     = "job_terminal_failure"
    PANIC_TRIGGER    = "panic_trigger"
    BURST_COLLISION  = "burst_collision"
    NODE_LAUNCHING   = "node_launching"
    NODE_READY       = "node_ready"
    NODE_IDLE        = "node_idle"
    NODE_DRAINING    = "node_draining"    # BSIM-85: node accepted no new jobs
    NODE_TERMINATED  = "node_terminated"
    COST_SAMPLE      = "cost_sample"
    CPU_WASTE        = "cpu_waste"     # BSIM-71: wasted vCPU-seconds per node
    POLICY_SWAP      = "policy_swap"   # BSIM-84: time-window boundary crossed


class PhaseID(str, Enum):
    DOWNLOAD   = "download"
    PREPROCESS = "preprocess"
    WORKHORSE  = "workhorse"
    UPLOAD     = "upload"
    QUEUED     = "queued"


class NodeState(str, Enum):
    LAUNCHING  = "launching"
    READY      = "ready"
    IDLE       = "idle"
    DRAINING   = "draining"    # BSIM-85: no new placements, finishes current jobs
    TERMINATED = "terminated"


@dataclass
class SimEvent:
    event_type: EventType
    sim_time: float
    data: dict[str, Any] = field(default_factory=dict)


class MetricsCollector:
    def __init__(self): self._log: list[SimEvent] = []
    def record(self, event: SimEvent): self._log.append(event)
    @property
    def log(self): return self._log
    def events_of_type(self, t): return [e for e in self._log if e.event_type == t]

    def save(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump([{"event_type": e.event_type.value,
                        "sim_time": e.sim_time, "data": e.data} for e in self._log], f, indent=2)

    def job_arrival(self, t, job_id, centroid_id):
        self.record(SimEvent(EventType.JOB_ARRIVAL, t, {"job_id": job_id, "centroid_id": centroid_id}))
    def job_queued(self, t, job_id, centroid_id, priority):
        self.record(SimEvent(EventType.JOB_QUEUED, t, {"job_id": job_id, "centroid_id": centroid_id, "priority": priority}))
    def job_start(self, t, job_id, centroid_id, node_id):
        self.record(SimEvent(EventType.JOB_START, t, {"job_id": job_id, "centroid_id": centroid_id, "node_id": node_id}))
    def phase_transition(self, t, job_id, phase, node_id):
        self.record(SimEvent(EventType.PHASE_TRANSITION, t, {"job_id": job_id, "phase": phase.value, "node_id": node_id}))
    def job_complete(self, t, job_id, centroid_id, node_id, queue_wait_s, total_elapsed_s, retry_count):
        self.record(SimEvent(EventType.JOB_COMPLETE, t, {"job_id": job_id, "centroid_id": centroid_id,
            "node_id": node_id, "queue_wait_s": queue_wait_s,
            "total_elapsed_s": total_elapsed_s, "retry_count": retry_count}))
    def job_crash(self, t, job_id, centroid_id, node_id, retry_count, reason):
        self.record(SimEvent(EventType.JOB_CRASH, t, {"job_id": job_id, "centroid_id": centroid_id,
            "node_id": node_id, "retry_count": retry_count, "reason": reason}))
    def job_terminal(self, t, job_id, centroid_id):
        self.record(SimEvent(EventType.JOB_TERMINAL, t, {"job_id": job_id, "centroid_id": centroid_id}))
    def panic_trigger(self, t, job_id, wait_s):
        self.record(SimEvent(EventType.PANIC_TRIGGER, t, {"job_id": job_id, "wait_s": wait_s}))
    def burst_collision(self, t, node_id, job_ids, victim_id, aggregate_ram_gb, node_ram_gb):
        self.record(SimEvent(EventType.BURST_COLLISION, t, {"node_id": node_id, "job_ids": job_ids,
            "victim_id": victim_id, "aggregate_ram_gb": aggregate_ram_gb, "node_ram_gb": node_ram_gb}))
    def node_launching(self, t, node_id, instance_name):
        self.record(SimEvent(EventType.NODE_LAUNCHING, t, {"node_id": node_id, "instance_name": instance_name}))
    def node_ready(self, t, node_id, instance_name):
        self.record(SimEvent(EventType.NODE_READY, t, {"node_id": node_id, "instance_name": instance_name}))
    def node_idle(self, t, node_id):
        self.record(SimEvent(EventType.NODE_IDLE, t, {"node_id": node_id}))
    def node_draining(self, t, node_id, drain_rule_idle_vcpu=None):
        self.record(SimEvent(EventType.NODE_DRAINING, t, {
            "node_id": node_id,
            **({"drain_rule_idle_vcpu": drain_rule_idle_vcpu} if drain_rule_idle_vcpu is not None else {})
        }))
    def node_terminated(self, t, node_id, idle_duration_s):
        self.record(SimEvent(EventType.NODE_TERMINATED, t, {"node_id": node_id, "idle_duration_s": idle_duration_s}))
    def policy_swap(self, t, old_start_s, new_start_s):
        self.record(SimEvent(EventType.POLICY_SWAP, t, {
            "old_window_start_s": old_start_s,
            "new_window_start_s": new_start_s,
        }))
