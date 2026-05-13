"""BSIM-37/38/39: Experiment runner — panic threshold sweep, Pareto frontier, meta-effect."""
from __future__ import annotations
import json, random
from pathlib import Path
from typing import Optional
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.console import Console
from batch_sim.core.schemas import SchedulerType
from batch_sim.core.engine import SimulationEngine
from batch_sim.generator.event_list import load_event_list
from batch_sim.metrics.collector import MetricsCollector
from batch_sim.metrics.aggregator import build_scorecard

console = Console()


def run_one(event_list, scheduler_type, cfg, registry, event_list_path,
            seed=42, return_metrics=False):
    """Run one scheduler configuration and return a Scorecard.
    If return_metrics=True, returns (Scorecard, MetricsCollector) instead.
    """
    metrics = MetricsCollector(); rng = random.Random(seed)
    if scheduler_type == SchedulerType.BATCH:
        from batch_sim.scheduler.batch_scheduler import BatchScheduler
        scheduler = BatchScheduler(cfg=cfg, registry=registry, metrics=metrics, rng=rng)
    else:
        centroid_peak_rams = list({e.preprocess_peak_ram_gb for e in event_list.events})
        from batch_sim.scheduler.k8s_scheduler import K8SScheduler
        scheduler = K8SScheduler(cfg=cfg, registry=registry, metrics=metrics,
                                  centroid_peak_rams=centroid_peak_rams, rng=rng)
    engine = SimulationEngine(scheduler=scheduler, metrics=metrics, cfg=cfg)
    cooloff = event_list.metadata.get("cooloff_seconds", 0.0)
    engine.run(event_list, cooloff_seconds=cooloff)
    scheduler.finalize(engine.env)
    k8s_cap = scheduler.capacity_report() if scheduler_type == SchedulerType.K8S and hasattr(scheduler, "capacity_report") else None
    sim_horizon = event_list.metadata.get("horizon_seconds", 0)
    sc = build_scorecard(scheduler_type=scheduler_type.value,
        panic_threshold_s=cfg.panic_threshold_seconds, event_list_path=event_list_path,
        collector=metrics, accruers=scheduler.accruers,
        sla_target_seconds=cfg.sla_target_seconds, sim_horizon=sim_horizon,
        k8s_capacity_report=k8s_cap)
    return (sc, metrics) if return_metrics else sc


def run_experiment(event_list_path, panic_threshold_values, base_cfg, registry,
                   output_dir, schedulers=None, seed=42):
    if schedulers is None: schedulers = [SchedulerType.BATCH, SchedulerType.K8S]
    output_dir = Path(output_dir); event_list = load_event_list(event_list_path)
    collated = []
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeElapsedColumn(), console=console) as progress:
        task = progress.add_task("Sweeping…", total=len(panic_threshold_values)*len(schedulers))
        for threshold in sorted(panic_threshold_values):
            for sched_type in schedulers:
                cfg = base_cfg.model_copy(update={"panic_threshold_seconds": threshold,
                                                    "scheduler_type": sched_type})
                progress.update(task, description=f"[{sched_type.value.upper():5s}] {threshold:.0f}s")
                sc = run_one(event_list=event_list, scheduler_type=sched_type, cfg=cfg,
                             registry=registry, event_list_path=event_list_path, seed=seed)
                run_dir = output_dir / sched_type.value / f"threshold_{int(threshold)}"
                sc.save(run_dir / "scorecard.json")
                collated.append({"scheduler": sched_type.value, "panic_threshold_s": threshold,
                    "total_cost_usd": sc.cost_summary.total_cost_usd,
                    "mean_wait_s": (sc.job_stats.pool_queue_wait_s or {}).get("mean", 0) or 0,
                    "sla_breach_count": sc.job_stats.pool_sla_breach_count,
                    "crash_count": sc.job_stats.pool_crash_count,
                    "panic_count": sc.job_stats.pool_panic_trigger_count,
                    "total_idle_s": sc.idle_decomposition.total_idle_s,
                    "post_last_job_idle_s": sc.idle_decomposition.post_last_job_s})
                progress.advance(task)
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "collated.json", "w") as f: json.dump(collated, f, indent=2)
    return collated


def build_pareto_frontier(collated, scheduler):
    runs = [r for r in collated if r["scheduler"] == scheduler]
    frontier = []
    for candidate in runs:
        dominated = any(
            other["total_cost_usd"] <= candidate["total_cost_usd"]
            and other["mean_wait_s"] <= candidate["mean_wait_s"]
            and (other["total_cost_usd"] < candidate["total_cost_usd"]
                 or other["mean_wait_s"] < candidate["mean_wait_s"])
            for other in runs if other is not candidate)
        candidate["pareto_dominated"] = dominated
        if not dominated: frontier.append(candidate)
    return frontier


def detect_meta_effect(collated):
    result = {}
    for sched in ("batch", "k8s"):
        runs = sorted([r for r in collated if r["scheduler"] == sched],
                      key=lambda r: r["panic_threshold_s"])
        if len(runs) < 3:
            result[sched] = {"inflection_threshold_s": None, "detected": False}; continue
        costs = [r["total_cost_usd"] for r in runs]
        thresholds = [r["panic_threshold_s"] for r in runs]
        inflection = next((thresholds[i] for i in range(1, len(costs)) if costs[i] > costs[i-1]), None)
        result[sched] = {"inflection_threshold_s": inflection, "detected": inflection is not None,
                         "costs_by_threshold": list(zip(thresholds, costs))}
    return result
