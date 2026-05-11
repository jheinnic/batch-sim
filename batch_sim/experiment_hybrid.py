"""
Hybrid OKD-Q1 / Batch-Q2 experiment runner.

Routes each job at arrival to either:
  - OKD K8S+ scheduler (Q1: advantage_ratio >= k AND peak fits Q1 MachineSet)
  - AWS Batch scheduler (Q2: advantage_ratio < k OR peak exceeds Q1 MachineSet)

The advantage ratio is computed against the fixed Q1 MachineSet instance type
(C = q1_instance.ram_gb), not the cheapest fitting instance per job.

Sweep axes: q1_instance_type × k_values
"""

from __future__ import annotations

import json
import random
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import simpy

from batch_sim.core.config_loader import load_scheduler_config
from batch_sim.core.engine import (
    NodeModel, JobQueue, Priority, OverloadHandler, run_job_process, SimulationEngine
)
from batch_sim.core.schemas import SchedulerConfig, SchedulerType, InstanceTypeConfig
from batch_sim.generator.event_list import EventList, load_event_list
from batch_sim.metrics.collector import MetricsCollector, EventType, NodeState as NodeStateEnum
from batch_sim.registry.instance_registry import (
    InstanceRegistry, NodeCostAccruer, compute_k8s_capacity, PoolCostSummary
)
from batch_sim.scheduler.batch_scheduler import BatchScheduler
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler
from batch_sim.scheduler.queue_router import compute_advantage_ratio, QueueClass
from batch_sim.metrics.aggregator import build_scorecard, compute_job_stats


# ---------------------------------------------------------------------------
# Hybrid router
# ---------------------------------------------------------------------------

@dataclass
class RoutingDecision:
    queue:            QueueClass
    advantage_ratio:  float
    reason:           str   # 'exceeds_q1', 'below_k', 'q1_eligible'


def route_job(
    peak_ram_gb:    float,
    steady_ram_gb:  float,
    q1_instance:    InstanceTypeConfig,
    os_overhead_gb: float,
    k:              float,
) -> RoutingDecision:
    """
    Determine whether a job goes to OKD (Q1) or Batch (Q2).

    C is fixed as q1_instance.ram_gb — the design-time MachineSet choice.
    """
    C = q1_instance.ram_gb - os_overhead_gb   # net Q1 node capacity

    if peak_ram_gb > C:
        # Cannot physically burst on Q1 nodes → Batch unconditionally
        return RoutingDecision(QueueClass.Q2, 0.0, 'exceeds_q1')

    ratio = compute_advantage_ratio(peak_ram_gb, steady_ram_gb, C)

    if ratio >= k:
        return RoutingDecision(QueueClass.Q1, ratio, 'q1_eligible')
    else:
        return RoutingDecision(QueueClass.Q2, ratio, 'below_k')


# ---------------------------------------------------------------------------
# Hybrid simulation engine
# ---------------------------------------------------------------------------

class HybridScheduler:
    """
    Thin router that dispatches arriving jobs to either the OKD K8S+
    scheduler or the Batch scheduler based on the advantage ratio.

    Both sub-schedulers run inside the same SimPy environment and share
    the same MetricsCollector, so all events appear in one log.
    """

    def __init__(
        self,
        cfg:            SchedulerConfig,
        registry:       InstanceRegistry,
        okd_metrics:    MetricsCollector,
        batch_metrics:  MetricsCollector,
        q1_instance:    InstanceTypeConfig,
        k:              float,
        centroid_peak_rams: list[float],
        rng:            Optional[random.Random] = None,
    ) -> None:
        self.cfg        = cfg
        self.q1_instance = q1_instance
        self.k          = k
        rng = rng or random.Random(42)

        # OKD K8S+ — only sees Q1 jobs; centroid_peak_rams filtered to Q1-eligible
        q1_peaks = [p for p in centroid_peak_rams
                    if p <= q1_instance.ram_gb - cfg.k8s_os_overhead_gb]
        self._okd = K8SPlusScheduler(
            cfg=cfg, registry=registry, metrics=okd_metrics,
            centroid_peak_rams=q1_peaks or centroid_peak_rams,
            rng=rng,
        )

        # Batch — receives Q2 jobs
        batch_cfg = cfg.model_copy(update={'scheduler_type': SchedulerType.BATCH})
        self._batch = BatchScheduler(
            cfg=batch_cfg, registry=registry, metrics=batch_metrics, rng=rng,
        )

        self.okd_metrics   = okd_metrics
        self.batch_metrics = batch_metrics

        # Routing log: job_id → RoutingDecision
        self.routing: dict[str, RoutingDecision] = {}

    def on_job_arrival(self, env, job, arrival_time):
        p      = job.profile
        dec    = route_job(
            peak_ram_gb=p.preprocess_peak_ram_gb,
            steady_ram_gb=p.preprocess_steady_ram_gb,
            q1_instance=self.q1_instance,
            os_overhead_gb=self.cfg.k8s_os_overhead_gb,
            k=self.k,
        )
        self.routing[job.job_id] = dec

        if dec.queue == QueueClass.Q1:
            self._okd.on_job_arrival(env, job, arrival_time)
        else:
            self._batch.on_job_arrival(env, job, arrival_time)

    def on_job_complete(self, env, node, job):
        # Delegate to whichever scheduler owns this node
        if job.job_id in self.routing and self.routing[job.job_id].queue == QueueClass.Q1:
            self._okd.on_job_complete(env, node, job)
        else:
            self._batch.on_job_complete(env, node, job)

    def guarantee_capacity(self, env, job):
        if job.job_id in self.routing and self.routing[job.job_id].queue == QueueClass.Q1:
            self._okd.guarantee_capacity(env, job)
        else:
            self._batch.guarantee_capacity(env, job)

    def finalize(self, env):
        self._okd.finalize(env)
        self._batch.finalize(env)

    @property
    def okd_accruers(self):
        return self._okd.accruers

    @property
    def batch_accruers(self):
        return self._batch.accruers

    def routing_summary(self) -> dict:
        decisions = list(self.routing.values())
        total     = len(decisions)
        q1        = [d for d in decisions if d.queue == QueueClass.Q1]
        q2_below  = [d for d in decisions if d.reason == 'below_k']
        q2_exceeds= [d for d in decisions if d.reason == 'exceeds_q1']
        ratios    = [d.advantage_ratio for d in decisions if d.advantage_ratio > 0]
        return {
            'total_jobs':       total,
            'q1_count':         len(q1),
            'q2_below_k':       len(q2_below),
            'q2_exceeds_q1':    len(q2_exceeds),
            'q1_pct':           round(len(q1)/total*100, 1) if total else 0,
            'q2_pct':           round((len(q2_below)+len(q2_exceeds))/total*100, 1) if total else 0,
            'ratio_mean':       round(statistics.mean(ratios), 3) if ratios else 0,
            'ratio_min':        round(min(ratios), 3) if ratios else 0,
            'ratio_max':        round(max(ratios), 3) if ratios else 0,
        }


# ---------------------------------------------------------------------------
# Single hybrid run
# ---------------------------------------------------------------------------

def run_hybrid(
    event_list:         EventList,
    cfg:                SchedulerConfig,
    registry:           InstanceRegistry,
    q1_instance:        InstanceTypeConfig,
    k:                  float,
    seed:               int = 42,
) -> dict:
    """Run one (q1_instance, k) configuration and return a result dict."""
    okd_metrics   = MetricsCollector()
    batch_metrics = MetricsCollector()
    centroid_peaks = list({e.preprocess_peak_ram_gb for e in event_list.events})
    rng = random.Random(seed)

    hybrid = HybridScheduler(
        cfg=cfg, registry=registry,
        okd_metrics=okd_metrics, batch_metrics=batch_metrics,
        q1_instance=q1_instance, k=k,
        centroid_peak_rams=centroid_peaks, rng=rng,
    )

    # Run both schedulers in the same SimPy environment via a shared dispatcher
    env = simpy.Environment()
    hybrid._okd._setup(env)
    hybrid._batch._setup(env)

    for event in event_list.events:
        env.process(_arrival_process(env, event, hybrid))
    env.run()
    hybrid.finalize(env)

    sim_horizon = event_list.metadata.get('horizon_seconds', 0)

    # OKD scorecard
    okd_sc = build_scorecard(
        scheduler_type='okd_k8s_plus',
        panic_threshold_s=cfg.panic_threshold_seconds,
        event_list_path='hybrid',
        collector=okd_metrics,
        accruers=hybrid.okd_accruers,
        sla_target_seconds=cfg.sla_target_seconds,
        sim_horizon=sim_horizon,
        k8s_capacity_report=hybrid._okd.capacity_report(),
    )

    # Batch scorecard
    batch_sc = build_scorecard(
        scheduler_type='batch',
        panic_threshold_s=cfg.panic_threshold_seconds,
        event_list_path='hybrid',
        collector=batch_metrics,
        accruers=hybrid.batch_accruers,
        sla_target_seconds=cfg.sla_target_seconds,
        sim_horizon=sim_horizon,
    )

    routing = hybrid.routing_summary()
    okd_cost   = okd_sc.cost_summary.total_cost_usd
    batch_cost = batch_sc.cost_summary.total_cost_usd

    return {
        'q1_instance':       q1_instance.name,
        'q1_instance_ram_gb': q1_instance.ram_gb,
        'q1_hourly_usd':     q1_instance.hourly_price_usd,
        'k':                 k,
        'routing':           routing,
        'okd_cost':          round(okd_cost, 4),
        'batch_cost':        round(batch_cost, 4),
        'combined_cost':     round(okd_cost + batch_cost, 4),
        'okd_jobs':          okd_sc.job_stats.pool_job_count,
        'okd_crashes':       okd_sc.job_stats.pool_crash_count,
        'okd_terminal':      okd_sc.job_stats.pool_terminal_failure_count,
        'okd_mean_wait':     round((okd_sc.job_stats.pool_queue_wait_s or {}).get('mean', 0), 1),
        'batch_jobs':        batch_sc.job_stats.pool_job_count,
        'batch_crashes':     batch_sc.job_stats.pool_crash_count,
        'batch_mean_wait':   round((batch_sc.job_stats.pool_queue_wait_s or {}).get('mean', 0), 1),
        'okd_cost_series':   [(round(t/3600,2), round(c,2))
                              for t,c in okd_sc.cost_summary.cost_over_time[::4]],
        'batch_cost_series': [(round(t/3600,2), round(c,2))
                              for t,c in batch_sc.cost_summary.cost_over_time[::4]],
    }


def _arrival_process(env, event, hybrid):
    yield env.timeout(event.arrival_time)
    job = event.to_job_spec()
    hybrid.okd_metrics.job_arrival(env.now, job.job_id, job.centroid_id)
    hybrid.on_job_arrival(env, job, arrival_time=event.arrival_time)


# ---------------------------------------------------------------------------
# Sweep runner
# ---------------------------------------------------------------------------

def run_hybrid_sweep(
    event_list_path: str,
    cfg:             SchedulerConfig,
    registry:        InstanceRegistry,
    q1_instance_names: list[str],
    k_values:        list[float],
    output_dir:      str | Path,
    seed:            int = 42,
) -> list[dict]:
    """
    Sweep (q1_instance × k) and also run pure Batch and pure OKD baselines.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    event_list = load_event_list(event_list_path)

    # Baselines first
    from batch_sim.experiment_runner import run_one
    results = []

    print("Running baselines...")
    for sched_type, label in [
        (SchedulerType.BATCH, 'batch_baseline'),
        (SchedulerType.K8S,   'k8s_baseline'),
    ]:
        sc = run_one(event_list, sched_type, cfg, registry, event_list_path, seed)
        results.append({
            'run_type':      label,
            'q1_instance':   'N/A',
            'k':             None,
            'combined_cost': round(sc.cost_summary.total_cost_usd, 4),
            'okd_cost':      round(sc.cost_summary.total_cost_usd, 4) if 'k8s' in label else None,
            'batch_cost':    round(sc.cost_summary.total_cost_usd, 4) if 'batch' in label else None,
            'okd_jobs':      sc.job_stats.pool_job_count if 'k8s' in label else None,
            'batch_jobs':    sc.job_stats.pool_job_count if 'batch' in label else None,
            'okd_crashes':   sc.job_stats.pool_crash_count if 'k8s' in label else None,
            'batch_crashes': sc.job_stats.pool_crash_count if 'batch' in label else None,
            'routing':       {'q1_pct': 100 if 'k8s' in label else 0,
                              'q2_pct': 0   if 'k8s' in label else 100},
        })
        print(f"  {label}: ${sc.cost_summary.total_cost_usd:.2f}")

    # Hybrid sweep
    instance_map = {i.name: i for i in registry.all_types}
    total = len(q1_instance_names) * len(k_values)
    done  = 0

    for inst_name in q1_instance_names:
        q1_inst = instance_map.get(inst_name)
        if q1_inst is None:
            print(f"  WARNING: {inst_name} not in registry — skipping")
            continue
        for k in k_values:
            done += 1
            print(f"  [{done}/{total}] Q1={inst_name} k={k}...", end=' ', flush=True)
            r = run_hybrid(event_list=event_list, cfg=cfg, registry=registry,
                           q1_instance=q1_inst, k=k, seed=seed)
            r['run_type'] = 'hybrid'
            results.append(r)
            print(f"combined=${r['combined_cost']:.2f}  "
                  f"OKD={r['routing']['q1_pct']}%  Batch={r['routing']['q2_pct']}%")

            # Save individual run
            run_dir = output_dir / inst_name / f"k{k}"
            run_dir.mkdir(parents=True, exist_ok=True)
            with open(run_dir / 'result.json', 'w') as f:
                json.dump(r, f, indent=2)

    with open(output_dir / 'collated.json', 'w') as f:
        json.dump(results, f, indent=2)

    return results
