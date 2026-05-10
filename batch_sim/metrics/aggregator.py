"""BSIM-32/33/34/35: Metrics aggregation, scorecard, and comparator."""
from __future__ import annotations
import json, statistics
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
from batch_sim.metrics.collector import MetricsCollector, EventType
from batch_sim.registry.instance_registry import NodeCostAccruer, PoolCostSummary


def _stats(values):
    if not values: return {"count": 0, "min": None, "max": None, "mean": None, "stddev": None}
    return {"count": len(values), "min": min(values), "max": max(values),
            "mean": statistics.mean(values),
            "stddev": statistics.stdev(values) if len(values) > 1 else 0.0}


@dataclass
class PerCentroidStats:
    centroid_id: str
    queue_wait_s: dict = field(default_factory=dict)
    total_elapsed_s: dict = field(default_factory=dict)
    retry_count: dict = field(default_factory=dict)
    sla_breach_count: int = 0
    crash_count: int = 0
    terminal_failure_count: int = 0
    job_count: int = 0


@dataclass
class JobStatsReport:
    per_centroid: dict = field(default_factory=dict)
    pool_queue_wait_s: dict = field(default_factory=dict)
    pool_total_elapsed_s: dict = field(default_factory=dict)
    pool_retry_count: dict = field(default_factory=dict)
    pool_sla_breach_count: int = 0
    pool_crash_count: int = 0
    pool_terminal_failure_count: int = 0
    pool_job_count: int = 0
    pool_panic_trigger_count: int = 0


def compute_job_stats(collector, sla_target_seconds):
    complete = collector.events_of_type(EventType.JOB_COMPLETE)
    crashes = collector.events_of_type(EventType.JOB_CRASH)
    terminals = collector.events_of_type(EventType.JOB_TERMINAL)
    panics = collector.events_of_type(EventType.PANIC_TRIGGER)
    cw, ce, cr, cb, cc, ct, cn = {}, {}, {}, {}, {}, {}, {}
    for e in complete:
        cid = e.data["centroid_id"]
        cw.setdefault(cid, []).append(e.data["queue_wait_s"])
        ce.setdefault(cid, []).append(e.data["total_elapsed_s"])
        cr.setdefault(cid, []).append(e.data["retry_count"])
        cb[cid] = cb.get(cid, 0) + (1 if e.data["queue_wait_s"] > sla_target_seconds else 0)
        cn[cid] = cn.get(cid, 0) + 1
    for e in crashes: cc[e.data["centroid_id"]] = cc.get(e.data["centroid_id"], 0) + 1
    for e in terminals: ct[e.data["centroid_id"]] = ct.get(e.data["centroid_id"], 0) + 1
    all_cids = set(list(cw) + list(cc) + list(ct))
    per_centroid = {cid: PerCentroidStats(centroid_id=cid,
        queue_wait_s=_stats(cw.get(cid, [])), total_elapsed_s=_stats(ce.get(cid, [])),
        retry_count=_stats([float(r) for r in cr.get(cid, [])]),
        sla_breach_count=cb.get(cid, 0), crash_count=cc.get(cid, 0),
        terminal_failure_count=ct.get(cid, 0), job_count=cn.get(cid, 0))
        for cid in all_cids}
    all_waits = [e.data["queue_wait_s"] for e in complete]
    return JobStatsReport(per_centroid=per_centroid,
        pool_queue_wait_s=_stats(all_waits),
        pool_total_elapsed_s=_stats([e.data["total_elapsed_s"] for e in complete]),
        pool_retry_count=_stats([float(e.data["retry_count"]) for e in complete]),
        pool_sla_breach_count=sum(1 for w in all_waits if w > sla_target_seconds),
        pool_crash_count=len(crashes), pool_terminal_failure_count=len(terminals),
        pool_job_count=len(complete), pool_panic_trigger_count=len(panics))


@dataclass
class IdleTimeDecomposition:
    total_idle_s: float = 0.0
    pre_first_job_s: float = 0.0
    between_jobs_s: float = 0.0
    post_last_job_s: float = 0.0


def compute_idle_decomposition(collector):
    ready = {e.data["node_id"]: e.sim_time for e in collector.events_of_type(EventType.NODE_READY)}
    term = {e.data["node_id"]: (e.sim_time, e.data["idle_duration_s"])
            for e in collector.events_of_type(EventType.NODE_TERMINATED)}
    starts = collector.events_of_type(EventType.JOB_START)
    first_start = {}
    for e in starts:
        nid = e.data["node_id"]
        if nid not in first_start or e.sim_time < first_start[nid]:
            first_start[nid] = e.sim_time
    pre = sum(max(0.0, first_start.get(nid, rt) - rt) for nid, rt in ready.items())
    post = sum(v[1] for v in term.values())
    return IdleTimeDecomposition(total_idle_s=pre + post, pre_first_job_s=pre,
                                  between_jobs_s=0.0, post_last_job_s=post)


@dataclass
class Scorecard:
    scheduler_type: str
    panic_threshold_s: float
    event_list_path: str
    job_stats: JobStatsReport
    cost_summary: PoolCostSummary
    idle_decomposition: IdleTimeDecomposition
    k8s_capacity_report: Optional[dict] = None

    def save(self, path):
        path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"scheduler_type": self.scheduler_type,
            "panic_threshold_s": self.panic_threshold_s,
            "event_list_path": self.event_list_path,
            "job_stats": asdict(self.job_stats),
            "cost_summary": {"total_cost_usd": self.cost_summary.total_cost_usd,
                "cost_by_family": self.cost_summary.cost_by_family,
                "cost_over_time": self.cost_summary.cost_over_time,
                "node_count_over_time": self.cost_summary.node_count_over_time},
            "idle_decomposition": asdict(self.idle_decomposition),
            "k8s_capacity_report": self.k8s_capacity_report}
        with open(path, "w") as f: json.dump(payload, f, indent=2)


def build_scorecard(scheduler_type, panic_threshold_s, event_list_path,
                    collector, accruers, sla_target_seconds, sim_horizon,
                    k8s_capacity_report=None):
    return Scorecard(scheduler_type=scheduler_type, panic_threshold_s=panic_threshold_s,
        event_list_path=event_list_path,
        job_stats=compute_job_stats(collector, sla_target_seconds),
        cost_summary=PoolCostSummary.from_accruers(accruers, sim_horizon=sim_horizon),
        idle_decomposition=compute_idle_decomposition(collector),
        k8s_capacity_report=k8s_capacity_report)


def compare_scorecards(batch_path, k8s_path):
    with open(batch_path) as f: batch = json.load(f)
    with open(k8s_path) as f: k8s = json.load(f)
    def delta(b, k, key):
        bv, kv = b.get(key), k.get(key)
        if bv is None or kv is None: return None
        return {"batch": bv, "k8s": kv, "delta": kv - bv,
                "ratio_k8s_batch": kv / bv if bv != 0 else None}
    return {
        "total_cost_usd": delta(batch["cost_summary"], k8s["cost_summary"], "total_cost_usd"),
        "pool_job_count": delta(batch["job_stats"], k8s["job_stats"], "pool_job_count"),
        "pool_sla_breach_count": delta(batch["job_stats"], k8s["job_stats"], "pool_sla_breach_count"),
        "pool_crash_count": delta(batch["job_stats"], k8s["job_stats"], "pool_crash_count"),
        "pool_panic_trigger_count": delta(batch["job_stats"], k8s["job_stats"], "pool_panic_trigger_count"),
        "pool_mean_wait_s": {"batch": (batch["job_stats"]["pool_queue_wait_s"] or {}).get("mean"),
                             "k8s": (k8s["job_stats"]["pool_queue_wait_s"] or {}).get("mean")},
        "idle_total_s": delta(batch["idle_decomposition"], k8s["idle_decomposition"], "total_idle_s"),
    }
