"""BSIM-17/18/19: Instance registry, K8S headroom calculation, and cost model."""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import yaml
from batch_sim.core.schemas import InstanceTypeConfig, InstanceRegistryConfig


class InstanceRegistry:
    def __init__(self, config: InstanceRegistryConfig) -> None:
        self._types = sorted(config.instance_types, key=lambda t: t.hourly_price_usd)

    @classmethod
    def from_yaml(cls, path: str | Path) -> InstanceRegistry:
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(InstanceRegistryConfig.model_validate(data))

    @property
    def all_types(self) -> list[InstanceTypeConfig]: return list(self._types)

    def get_by_name(self, name: str) -> Optional[InstanceTypeConfig]:
        for t in self._types:
            if t.name == name:
                return t
        return None

    def cheapest_fitting(self, min_ram_gb: float, min_vcpu: int) -> Optional[InstanceTypeConfig]:
        for t in self._types:
            if t.ram_gb >= min_ram_gb and t.vcpu >= min_vcpu:
                return t
        return None

    def candidates(self, min_ram_gb: float, min_vcpu: int) -> list[InstanceTypeConfig]:
        return [t for t in self._types if t.ram_gb >= min_ram_gb and t.vcpu >= min_vcpu]


@dataclass
class K8SCapacityProfile:
    instance: InstanceTypeConfig
    tier_local_mm_gb: float
    spike_headroom_gb: float
    os_overhead_gb: float
    effective_schedulable_gb: float
    soft_limit_gb: float
    max_schedulable_jobs: int
    headroom_pct: float


def compute_k8s_capacity(
    instance: InstanceTypeConfig,
    centroid_peak_rams: list[float],
    os_overhead_gb: float = 2.0,
) -> K8SCapacityProfile:
    fitting = [r for r in centroid_peak_rams if r <= instance.ram_gb - os_overhead_gb]
    if not fitting:
        return K8SCapacityProfile(instance=instance, tier_local_mm_gb=0.0,
            spike_headroom_gb=0.0, os_overhead_gb=os_overhead_gb,
            effective_schedulable_gb=0.0, soft_limit_gb=0.0,
            max_schedulable_jobs=0, headroom_pct=0.0)
    mm = max(fitting)
    spike = mm
    effective = max(instance.ram_gb - os_overhead_gb - spike, 0.0)
    soft = 0.08 * mm
    max_jobs = int(effective // soft) if soft > 0 and effective > 0 else 0
    return K8SCapacityProfile(instance=instance, tier_local_mm_gb=mm,
        spike_headroom_gb=spike, os_overhead_gb=os_overhead_gb,
        effective_schedulable_gb=effective, soft_limit_gb=soft,
        max_schedulable_jobs=max_jobs,
        headroom_pct=(spike / instance.ram_gb) * 100.0)


def batch_max_jobs(instance: InstanceTypeConfig, peak_ram_gb: float, declared_vcpu: int) -> int:
    if peak_ram_gb <= 0 or declared_vcpu <= 0:
        return 0
    return min(int(instance.ram_gb // peak_ram_gb), instance.vcpu // declared_vcpu)


@dataclass
class NodeCostAccruer:
    node_id: str
    instance: InstanceTypeConfig
    launch_time: float
    termination_time: float = -1.0

    @property
    def is_terminated(self) -> bool: return self.termination_time >= 0

    def terminate(self, at_time: float) -> None:
        if not self.is_terminated:
            self.termination_time = at_time

    @property
    def total_cost_usd(self) -> float:
        if not self.is_terminated:
            return 0.0
        return (self.termination_time - self.launch_time) / 3600.0 * self.instance.hourly_price_usd


@dataclass
class PoolCostSummary:
    total_cost_usd: float = 0.0
    cost_by_family: dict[str, float] = field(default_factory=dict)
    cost_over_time: list[tuple[float, float]] = field(default_factory=list)
    node_count_over_time: list[tuple[float, int]] = field(default_factory=list)

    @classmethod
    def from_accruers(
        cls,
        accruers: list[NodeCostAccruer],
        sample_interval_s: float = 60.0,
        sim_horizon: float = 0.0,
    ) -> PoolCostSummary:
        terminated = [a for a in accruers if a.is_terminated]
        total = sum(a.total_cost_usd for a in terminated)
        by_family: dict[str, float] = {}
        for a in terminated:
            fam = a.instance.family.value
            by_family[fam] = by_family.get(fam, 0.0) + a.total_cost_usd
        end_t = max((a.termination_time for a in terminated), default=sim_horizon)
        end_t = max(end_t, sim_horizon)
        cost_series: list[tuple[float, float]] = []
        count_series: list[tuple[float, int]] = []
        t = 0.0
        while t <= end_t:
            cost_at_t = sum(
                (min(t, a.termination_time) - a.launch_time) / 3600.0 * a.instance.hourly_price_usd
                for a in terminated if a.launch_time <= t)
            nodes_at_t = sum(1 for a in accruers
                if a.launch_time <= t and (not a.is_terminated or a.termination_time > t))
            cost_series.append((t, cost_at_t))
            count_series.append((t, nodes_at_t))
            t += sample_interval_s
        return cls(total_cost_usd=total, cost_by_family=by_family,
                   cost_over_time=cost_series, node_count_over_time=count_series)
