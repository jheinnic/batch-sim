"""BSIM-31: Typed simulation events and central metrics collector."""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional


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
    COST_SAMPLE              = "cost_sample"
    CPU_WASTE                = "cpu_waste"              # BSIM-71: wasted vCPU-seconds per node
    POLICY_SWAP              = "policy_swap"            # BSIM-84: time-window boundary crossed
    STORAGE_POOL_EXPANDED    = "storage_pool_expanded"  # BSIM-92: pool grew by one volume
    STORAGE_EXHAUSTED        = "storage_exhausted"      # BSIM-92: volume ceiling reached
    STORAGE_GEN_OPENED       = "storage_gen_opened"     # BSIM-93: new K8S pool generation
    STORAGE_GEN_RELEASED     = "storage_gen_released"   # BSIM-93: K8S generation fully freed
    ADMISSION_REJECTED       = "admission_rejected"     # BSIM-102/108: burst fits no compatible tier
    TIER_COMPATIBILITY_WARN  = "tier_compatibility_warn" # BSIM-108: declared tier cannot host job burst


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
    def __init__(self) -> None: self._log: list[SimEvent] = []
    def record(self, event: SimEvent) -> None: self._log.append(event)
    @property
    def log(self) -> list[SimEvent]: return self._log
    def events_of_type(self, t: EventType) -> list[SimEvent]:
        return [e for e in self._log if e.event_type == t]

    def save(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump([{"event_type": e.event_type.value,
                        "sim_time": e.sim_time, "data": e.data} for e in self._log], f, indent=2)

    def job_arrival(self, t: float, job_id: str, centroid_id: str) -> None:
        self.record(SimEvent(EventType.JOB_ARRIVAL, t, {"job_id": job_id, "centroid_id": centroid_id}))
    def job_queued(self, t: float, job_id: str, centroid_id: str, priority: str,
                   queue_name: Optional[str] = None) -> None:
        data: dict[str, Any] = {"job_id": job_id, "centroid_id": centroid_id, "priority": priority}
        if queue_name is not None:
            data["queue_name"] = queue_name
        self.record(SimEvent(EventType.JOB_QUEUED, t, data))
    def job_start(self, t: float, job_id: str, centroid_id: str, node_id: str) -> None:
        self.record(SimEvent(EventType.JOB_START, t, {"job_id": job_id, "centroid_id": centroid_id, "node_id": node_id}))
    def phase_transition(self, t: float, job_id: str, phase: PhaseID, node_id: str) -> None:
        self.record(SimEvent(EventType.PHASE_TRANSITION, t, {"job_id": job_id, "phase": phase.value, "node_id": node_id}))
    def job_complete(self, t: float, job_id: str, centroid_id: str, node_id: str,
                     queue_wait_s: float, total_elapsed_s: float, retry_count: int) -> None:
        self.record(SimEvent(EventType.JOB_COMPLETE, t, {"job_id": job_id, "centroid_id": centroid_id,
            "node_id": node_id, "queue_wait_s": queue_wait_s,
            "total_elapsed_s": total_elapsed_s, "retry_count": retry_count}))
    def job_crash(self, t: float, job_id: str, centroid_id: str, node_id: str,
                  retry_count: int, reason: str) -> None:
        self.record(SimEvent(EventType.JOB_CRASH, t, {"job_id": job_id, "centroid_id": centroid_id,
            "node_id": node_id, "retry_count": retry_count, "reason": reason}))
    def job_terminal(self, t: float, job_id: str, centroid_id: str) -> None:
        self.record(SimEvent(EventType.JOB_TERMINAL, t, {"job_id": job_id, "centroid_id": centroid_id}))
    def panic_trigger(self, t: float, job_id: str, wait_s: float) -> None:
        self.record(SimEvent(EventType.PANIC_TRIGGER, t, {"job_id": job_id, "wait_s": wait_s}))
    def burst_collision(self, t: float, node_id: str, job_ids: list[str], victim_id: str,
                        aggregate_ram_gb: float, node_ram_gb: float) -> None:
        self.record(SimEvent(EventType.BURST_COLLISION, t, {"node_id": node_id, "job_ids": job_ids,
            "victim_id": victim_id, "aggregate_ram_gb": aggregate_ram_gb, "node_ram_gb": node_ram_gb}))
    def node_launching(self, t: float, node_id: str, instance_name: str,
                       tier_name: Optional[str] = None) -> None:
        self.record(SimEvent(EventType.NODE_LAUNCHING, t, {
            "node_id": node_id, "instance_name": instance_name,
            **({"tier_name": tier_name} if tier_name else {}),
        }))
    def node_ready(self, t: float, node_id: str, instance_name: str) -> None:
        self.record(SimEvent(EventType.NODE_READY, t, {"node_id": node_id, "instance_name": instance_name}))
    def node_idle(self, t: float, node_id: str) -> None:
        self.record(SimEvent(EventType.NODE_IDLE, t, {"node_id": node_id}))
    def node_draining(self, t: float, node_id: str, drain_rule_idle_vcpu: Optional[float] = None) -> None:
        self.record(SimEvent(EventType.NODE_DRAINING, t, {
            "node_id": node_id,
            **({"drain_rule_idle_vcpu": drain_rule_idle_vcpu} if drain_rule_idle_vcpu is not None else {})
        }))
    def node_terminated(self, t: float, node_id: str, idle_duration_s: float) -> None:
        self.record(SimEvent(EventType.NODE_TERMINATED, t, {"node_id": node_id, "idle_duration_s": idle_duration_s}))
    def policy_swap(self, t: float, old_start_s: float, new_start_s: float) -> None:
        self.record(SimEvent(EventType.POLICY_SWAP, t, {
            "old_window_start_s": old_start_s,
            "new_window_start_s": new_start_s,
        }))

    def storage_pool_expanded(self, t: float, node_id: str, old_gb: float,
                               new_gb: float, committed_gb: float, trigger_pct: float) -> None:
        self.record(SimEvent(EventType.STORAGE_POOL_EXPANDED, t, {
            "node_id": node_id, "old_gb": old_gb, "new_gb": new_gb,
            "committed_gb": committed_gb, "trigger_pct": trigger_pct,
        }))

    def storage_exhausted(self, t: float, node_id: str,
                           committed_gb: float, capacity_gb: float) -> None:
        self.record(SimEvent(EventType.STORAGE_EXHAUSTED, t, {
            "node_id": node_id, "committed_gb": committed_gb, "capacity_gb": capacity_gb,
        }))

    def storage_gen_opened(self, t: float, node_id: str, gen_id: int,
                            capacity_gb: float, trigger_committed_pct: float) -> None:
        self.record(SimEvent(EventType.STORAGE_GEN_OPENED, t, {
            "node_id": node_id, "gen_id": gen_id,
            "capacity_gb": capacity_gb, "trigger_committed_pct": trigger_committed_pct,
        }))

    def storage_gen_released(self, t: float, node_id: str, gen_id: int,
                              capacity_gb: float, lifetime_s: float, jobs_served: int) -> None:
        self.record(SimEvent(EventType.STORAGE_GEN_RELEASED, t, {
            "node_id": node_id, "gen_id": gen_id, "capacity_gb": capacity_gb,
            "lifetime_s": lifetime_s, "jobs_served": jobs_served,
        }))
