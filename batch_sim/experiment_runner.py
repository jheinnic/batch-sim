from __future__ import annotations
import json, random
from pathlib import Path
from typing import Any, Optional, Union
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.console import Console
from batch_sim.core.schemas import SchedulerType
from batch_sim.core.engine import SimulationEngine
from batch_sim.generator.event_list import EventList, load_event_list
from batch_sim.metrics.collector import MetricsCollector
from batch_sim.metrics.aggregator import Scorecard, build_scorecard
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.scheduler.k8s_scheduler import K8SScheduler

console = Console()


def _tier_config_from_metadata(metadata: dict[str, Any]) -> "dict[str, dict] | None":
    """BSIM-105: Read centroid_tier_config from event-list metadata.

    Old event lists carry centroid_queue_config (single-queue) instead; promote
    each single-queue value to a single-element compatible-tier set so the tier
    scheduler can consume them unchanged.
    """
    tier_cfg = metadata.get("centroid_tier_config")
    if tier_cfg is not None:
        return tier_cfg
    queue_cfg = metadata.get("centroid_queue_config")
    if queue_cfg is None:
        return None

    def promote(qn: "str | list | None") -> "list | None":
        if isinstance(qn, str):
            return [qn]
        return None  # list (per-bin) handled via *_by_bin below

    promoted: dict[str, dict] = {}
    for cid, c in queue_cfg.items():
        qn = c.get("queue_name")
        promoted[cid] = {
            "compatible_tiers": promote(qn),
            "window_overrides": [
                {
                    "start_time_s": w["start_time_s"],
                    "end_time_s": w["end_time_s"],
                    "compatible_tiers": promote(w.get("queue_name")),
                    "compatible_tiers_by_bin": (
                        [[q] for q in w["queue_name"]]
                        if isinstance(w.get("queue_name"), list) else None
                    ),
                }
                for w in c.get("window_overrides", [])
            ],
        }
    return promoted


def run_one(
    event_list: EventList,
    cfg: Any,
    registry: InstanceRegistry,
    event_list_path: str | Path,
    seed: int = 42,
    return_metrics: bool = False,
) -> Union[Scorecard, tuple[Scorecard, MetricsCollector]]:
    """Run one scheduler configuration and return a Scorecard.
    If return_metrics=True, returns (Scorecard, MetricsCollector) instead.

    BSIM-123: the scheduler is intrinsic to the config (cfg is a BatchConfig /
    K8SConfig / K8SPlusConfig), so the type is read from cfg.scheduler_type — no
    separate argument that could disagree with the config.
    """
    scheduler_type = cfg.scheduler_type
    metrics = MetricsCollector(); rng = random.Random(seed)
    if (scheduler_type == SchedulerType.BATCH):
        from batch_sim.scheduler.batch_scheduler import BatchScheduler
        scheduler = BatchScheduler(cfg=cfg, registry=registry, metrics=metrics, rng=rng)
    elif (scheduler_type == SchedulerType.K8S):
        centroid_peak_rams = list({e.preprocess_peak_ram_gb for e in event_list.events})
        centroid_tier_config = _tier_config_from_metadata(event_list.metadata)
        scheduler = K8SScheduler(cfg=cfg, registry=registry, metrics=metrics,
                                 centroid_peak_rams=centroid_peak_rams,
                                 centroid_tier_config=centroid_tier_config, rng=rng)
    elif (scheduler_type == SchedulerType.K8SPLUS):
        centroid_peak_rams = list({e.preprocess_peak_ram_gb for e in event_list.events})
        centroid_tier_config = _tier_config_from_metadata(event_list.metadata)
        from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler
        scheduler = K8SPlusScheduler(cfg=cfg, registry=registry, metrics=metrics,
                                     centroid_peak_rams=centroid_peak_rams,
                                     centroid_tier_config=centroid_tier_config, rng=rng)
    else:
        raise ValueError(f"Unknown scheduler type: {scheduler_type}")
    engine = SimulationEngine(scheduler=scheduler, metrics=metrics, cfg=cfg)
    cool_off = event_list.metadata.get("cool_off_seconds", 0.0)
    engine.run(event_list, cool_off_seconds=cool_off)
    scheduler.finalize(engine.env)
    if isinstance(scheduler, K8SScheduler):
        k8s_cap = scheduler.capacity_report()
    else:
        k8s_cap = None
    sim_horizon = event_list.metadata.get("horizon_seconds", 0)
    storage_pools = getattr(scheduler, "storage_pools", None)
    sc = build_scorecard(scheduler_type=scheduler_type.value,
        event_list_path=event_list_path,
        collector=metrics, accruers=scheduler.accruers,
        sla_target_seconds=cfg.sla_target_seconds, sim_horizon=sim_horizon,
        k8s_capacity_report=k8s_cap, storage_pools=storage_pools)
    return (sc, metrics) if return_metrics else sc


def run_experiment(
    event_list_path: str | Path,
    threshold_values: list[float],
    base_cfg: Any,
    registry: InstanceRegistry,
    output_dir: str | Path,
    schedulers: Optional[list[SchedulerType]] = None,
    seed: int = 42,
) -> list[dict[str, Any]]:
    # BSIM-109/E23: the cross-scheduler sweep morphed one base config across
    # scheduler types via model_copy(scheduler_type=...). That is impossible now that
    # each scheduler is a distinct discriminated-union subclass (a BatchConfig cannot
    # become a K8SConfig). Superseded by E23's declarative orchestration (named
    # workload × named scheduler grid); deferred per the E21/E23 same-deliverable
    # cadence. The single-scheduler run_one path is unaffected.
    #
    # The sweep axis this function used to vary was panic_threshold_seconds, which
    # no longer exists on any scheduler config -- no real AWS Batch or Kubernetes/
    # Karpenter autoscaler escalates job priority or forces capacity purely as a
    # function of elapsed queue wait, so the mechanism was removed rather than
    # repaired. There is currently no sweep axis to put in its place; reintroducing
    # one (e.g. a real, declared queue-priority scheme) is E23-or-later scope.
    raise NotImplementedError(
        "run_experiment's cross-scheduler sweep is retired pending E23 orchestration "
        "(BSIM-118-121) and has no sweep axis since panic_threshold_seconds was "
        "removed. Use run_one per scheduler, or the forthcoming ExperimentManifest "
        "orchestrator."
    )


def build_pareto_frontier(collated: list[dict[str, Any]], scheduler: str) -> list[dict[str, Any]]:
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


def detect_meta_effect(collated: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
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
